package com.tongji.relation.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEventWriterTest {

    @Mock
    private OutboxMapper outboxMapper;

    private OutboxEventWriter writer;

    @BeforeEach
    void setUp() {
        writer = new OutboxEventWriter(outboxMapper, new ObjectMapper());
    }

    @Test
    void writesSerializedPayloadWhenExactlyOneRowIsInserted() {
        when(outboxMapper.insert(eq(1L), eq("knowpost"), eq(2L), eq("KnowPostPublished"), anyString()))
                .thenReturn(1);

        writer.write(1L, "knowpost", 2L, "KnowPostPublished", Map.of("id", 2L));

        verify(outboxMapper).insert(
                eq(1L),
                eq("knowpost"),
                eq(2L),
                eq("KnowPostPublished"),
                eq("{\"id\":2}")
        );
    }

    @Test
    void throwsWhenInsertDoesNotAffectExactlyOneRow() {
        when(outboxMapper.insert(eq(1L), eq("following"), eq(2L), eq("FollowCreated"), anyString()))
                .thenReturn(0);

        assertThatThrownBy(() -> writer.write(
                1L,
                "following",
                2L,
                "FollowCreated",
                Map.of("id", 2L)
        ))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("inserted 0");
    }
}
