package com.tongji.knowpost.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.tongji.cache.hotkey.HotKeyDetector;
import com.tongji.counter.service.CounterService;
import com.tongji.knowpost.api.dto.FeedPageResponse;
import com.tongji.knowpost.mapper.KnowPostMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KnowPostFeedServiceImplTest {

    @Mock
    private KnowPostMapper mapper;
    @Mock
    private StringRedisTemplate redis;
    @Mock
    private ListOperations<String, String> listOperations;
    @Mock
    private ValueOperations<String, String> valueOperations;
    @Mock
    private CounterService counterService;
    @Mock
    private HotKeyDetector hotKeyDetector;

    private KnowPostFeedServiceImpl service;

    @BeforeEach
    void setUp() {
        Cache<String, FeedPageResponse> publicCache = Caffeine.newBuilder().build();
        Cache<String, FeedPageResponse> mineCache = Caffeine.newBuilder().build();

        when(redis.opsForList()).thenReturn(listOperations);
        when(redis.opsForValue()).thenReturn(valueOperations);
        when(listOperations.range(anyString(), anyLong(), anyLong())).thenReturn(List.of());

        service = new KnowPostFeedServiceImpl(
                mapper,
                redis,
                new ObjectMapper(),
                counterService,
                publicCache,
                mineCache,
                hotKeyDetector
        );
    }

    @Test
    void releasesSingleFlightEntryWhenDatabaseFallbackFails() throws Exception {
        when(mapper.listFeedPublic(anyInt(), anyInt()))
                .thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(() -> service.getPublicFeed(1, 20, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("database unavailable");

        assertThat(singleFlightEntries()).isEmpty();
        verify(mapper).listFeedPublic(21, 0);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, Object> singleFlightEntries() throws Exception {
        Field field = KnowPostFeedServiceImpl.class.getDeclaredField("singleFlight");
        field.setAccessible(true);
        return (ConcurrentHashMap<String, Object>) field.get(service);
    }
}
