package com.loopers.infrastructure.product;

import com.loopers.domain.product.ProductCursor;
import com.loopers.domain.product.ProductMetricsRepository;
import com.loopers.domain.product.ProductSortType;
import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.OrderSpecifier;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Component
public class ProductMetricsRepositoryImpl implements ProductMetricsRepository {

    private final ProductMetricsJpaRepository productMetricsJpaRepository;
    private final JPAQueryFactory queryFactory;

    /**
     * 키셋(커서) 페이지네이션 — QueryDSL 동적 쿼리. [등치(deleted_at, brand_id)] → [정렬 커서 비교식] →
     * [tie-break(product_id)] 순서로 week5 DESC 복합 인덱스(idx_pm_(brand_)active_likes_desc)의 물리 순서를
     * 그대로 타 filesort 를 피한다. hasNext 판별용으로 size+1 건을 읽어 상위에서 잘라낸다.
     */
    @Override
    public List<Long> findActiveIdsPage(Long brandId, ProductSortType sort, ProductCursor cursor, int size) {
        ProductSortType effective = (sort == null) ? ProductSortType.LATEST : sort;
        QProductMetricsEntity m = QProductMetricsEntity.productMetricsEntity;

        BooleanBuilder where = new BooleanBuilder();
        where.and(m.deletedAt.isNull());
        if (brandId != null) {
            where.and(m.brandId.eq(brandId));
        }
        where.and(cursorPredicate(m, effective, cursor));

        return queryFactory.select(m.productId)
                .from(m)
                .where(where)
                .orderBy(orderSpecifiers(m, effective))
                .limit(size + 1L)
                .fetch();
    }

    @Override
    public Map<Long, Long> findLikeCounts(Collection<Long> productIds) {
        if (productIds == null || productIds.isEmpty()) {
            return Map.of();
        }
        return productMetricsJpaRepository.findByProductIdIn(productIds).stream()
                .collect(Collectors.toMap(ProductMetricsEntity::getProductId, ProductMetricsEntity::getLikeCount));
    }

    @Override
    public long getLikeCount(Long productId) {
        return productMetricsJpaRepository.findById(productId)
                .map(ProductMetricsEntity::getLikeCount)
                .orElse(0L);
    }

    /**
     * 키셋 비교식 — 마지막으로 읽은 행(커서) "다음"부터 읽게 한다. cursor 가 null 이면 첫 페이지(조건 없음).
     * 정렬 컬럼 동점 구간은 product_id DESC 로 잘라(tie-break) 페이지 경계 누락·중복을 막는다.
     */
    private static BooleanExpression cursorPredicate(QProductMetricsEntity m, ProductSortType sort, ProductCursor cursor) {
        if (cursor == null) {
            return null; // BooleanBuilder.and(null) 은 무시되어 첫 페이지가 된다.
        }
        Long v = cursor.sortValue();
        Long lastId = cursor.id();
        return switch (sort) {
            case LATEST -> m.productId.lt(lastId);
            case LIKES_DESC -> m.likeCount.lt(v).or(m.likeCount.eq(v).and(m.productId.lt(lastId)));
            case PRICE_DESC -> m.price.lt(v).or(m.price.eq(v).and(m.productId.lt(lastId)));
            case PRICE_ASC -> m.price.gt(v).or(m.price.eq(v).and(m.productId.lt(lastId)));
        };
    }

    /** 정렬 + product_id DESC tie-break (페이지 경계 안정성 — 01 §7.2). */
    private static OrderSpecifier<?>[] orderSpecifiers(QProductMetricsEntity m, ProductSortType sort) {
        return switch (sort) {
            case LATEST -> new OrderSpecifier<?>[]{m.productId.desc()};
            case LIKES_DESC -> new OrderSpecifier<?>[]{m.likeCount.desc(), m.productId.desc()};
            case PRICE_DESC -> new OrderSpecifier<?>[]{m.price.desc(), m.productId.desc()};
            case PRICE_ASC -> new OrderSpecifier<?>[]{m.price.asc(), m.productId.desc()};
        };
    }
}
