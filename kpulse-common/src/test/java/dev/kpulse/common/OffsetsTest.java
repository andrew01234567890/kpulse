package dev.kpulse.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OffsetsTest {

    @Test
    void baseOffsetIsTheStampedIndexMinusTheBatchSpan() {
        assertThat(Offsets.baseOffset(2, 3)).isZero();
        assertThat(Offsets.baseOffset(4, 2)).isEqualTo(3);
        assertThat(Offsets.baseOffset(0, 1)).isZero();
    }

    @Test
    void logEndOffsetIsIndexPlusOneAndZeroForAnEmptyLog() {
        assertThat(Offsets.logEndOffset(-1)).isZero();
        assertThat(Offsets.logEndOffset(0)).isEqualTo(1);
        assertThat(Offsets.logEndOffset(4)).isEqualTo(5);
    }

    @Test
    void baseOffsetRejectsAnEmptyBatch() {
        assertThatThrownBy(() -> Offsets.baseOffset(0, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
