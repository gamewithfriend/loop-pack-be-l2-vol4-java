package com.loopers.infrastructure.product;

import com.loopers.domain.product.ProductModel;
import com.loopers.domain.product.ProductRepository;
import com.loopers.domain.product.ProductSortType;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

@RequiredArgsConstructor
@Component
public class ProductRepositoryImpl implements ProductRepository {
    private final ProductJpaRepository productJpaRepository;
    private final ProductMetricsJpaRepository productMetricsJpaRepository;

    /**
     * 순수 도메인 ↔ JPA 엔티티 경계.
     * - 신규(id == null): 매퍼로 엔티티를 만들어 INSERT.
     * - 기존(id != null): managed 엔티티를 로드해 가변 상태(이름/설명/이미지/가격/좋아요 수)만 복사 → dirty checking으로 UPDATE.
     *   soft delete 상태(deletedAt)도 도메인 기준으로 delete()/restore() 동기화한다(둘 다 멱등).
     *   (BaseEntity의 id가 final이라 도메인을 그대로 새 엔티티로 만들면 INSERT로 오인되므로 이 경로가 필요하다.)
     */
    /**
     * product 저장의 단일 관문. 여기서 product_metrics(read model)의 <b>차원 컬럼</b>도 함께 동기화한다
     * (생성/수정/삭제/복원 + Brand→Product cascade 삭제까지 모두 이 경로를 타므로 한 곳에서 커버).
     * 측정값 컬럼은 건드리지 않아 streamer 누적분이 보존된다.
     */
    @Override
    public ProductModel save(ProductModel product) {
        if (product.getId() == null) {
            ProductEntity saved = productJpaRepository.save(ProductEntityMapper.toEntity(product));
            productMetricsJpaRepository.insertDimensions(saved.getId(), saved.getBrandId(), saved.getPrice());
            return ProductEntityMapper.toDomain(saved);
        }
        ProductEntity entity = productJpaRepository.findById(product.getId())
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "[id = " + product.getId() + "] 상품을 찾을 수 없습니다."));
        entity.applyState(product.getName(), product.getDescription(), product.getImageUrl(),
                product.getPrice(), product.getLikesCount());
        if (product.isActive()) {
            entity.restore();
        } else {
            entity.delete();
        }
        ProductEntity persisted = productJpaRepository.save(entity);
        productMetricsJpaRepository.updateDimensions(
                persisted.getId(), persisted.getPrice(), persisted.getDeletedAt(), ZonedDateTime.now());
        return ProductEntityMapper.toDomain(persisted);
    }

    @Override
    public Optional<ProductModel> find(Long id) {
        return productJpaRepository.findById(id).map(ProductEntityMapper::toDomain);
    }

    @Override
    public List<ProductModel> findActiveByBrandId(Long brandId) {
        return productJpaRepository.findByBrandIdAndDeletedAtIsNull(brandId).stream()
                .map(ProductEntityMapper::toDomain)
                .toList();
    }

    @Override
    public List<ProductModel> findActiveByIds(Collection<Long> ids) {
        if (ids == null || ids.isEmpty()) return List.of();
        return productJpaRepository.findByIdInAndDeletedAtIsNull(ids).stream()
                .map(ProductEntityMapper::toDomain)
                .toList();
    }

    @Override
    public void incrementLikesCount(Long id) {
        productJpaRepository.incrementLikesCount(id);
    }

    @Override
    public void decrementLikesCount(Long id) {
        productJpaRepository.decrementLikesCount(id);
    }

    @Override
    public List<ProductModel> findActivePage(Long brandId, ProductSortType sort, int page, int size) {
        Pageable pageable = PageRequest.of(page, size, toSort(sort));
        List<ProductEntity> entities = (brandId == null)
                ? productJpaRepository.findByDeletedAtIsNull(pageable)
                : productJpaRepository.findByBrandIdAndDeletedAtIsNull(brandId, pageable);
        return entities.stream().map(ProductEntityMapper::toDomain).toList();
    }

    /** 정렬 + id DESC tiebreaker로 페이지 경계 안정성 보장 (01 §7.2). */
    private static Sort toSort(ProductSortType sort) {
        Sort byIdDesc = Sort.by(Sort.Direction.DESC, "id");
        ProductSortType effective = (sort == null) ? ProductSortType.LATEST : sort;
        return switch (effective) {
            case LATEST -> byIdDesc;
            case PRICE_ASC -> Sort.by(Sort.Direction.ASC, "price").and(byIdDesc);
            case PRICE_DESC -> Sort.by(Sort.Direction.DESC, "price").and(byIdDesc);
            case LIKES_DESC -> Sort.by(Sort.Direction.DESC, "likesCount").and(byIdDesc);
        };
    }
}
