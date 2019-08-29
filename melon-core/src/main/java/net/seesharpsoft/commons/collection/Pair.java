package net.seesharpsoft.commons.collection;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

public class Pair<K, T> {
    public static <K,T> Pair<K, T> ordered(K first, T second) {
        return new Pair(first, second, true);
    }

    public static <K,T> Pair<K, T> unordered(K first, T second) {
        return new Pair(first, second, false);
    }

    public static <K,T> Pair<K, T> of(K first, T second) {
        return ordered(first, second);
    }

    public static <K,T> Pair<K, T> of(K first, T second, boolean ordered) {
        return new Pair(first, second, ordered);
    }

    @Getter
    @Setter
    private K first;

    @Getter
    @Setter
    private T second;

    @Getter(AccessLevel.PROTECTED)
    @Setter(AccessLevel.PROTECTED)
    private boolean ordered = true;

    private Pair(K first, T second, boolean ordered) {
        setFirst(first);
        setSecond(second);
        setOrdered(ordered);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair)) {
            return false;
        }

        Pair otherPair = (Pair)obj;
        return Objects.equals(this.isOrdered(), otherPair.isOrdered()) && (
                (Objects.equals(this.getFirst(), otherPair.getFirst()) && Objects.equals(this.getSecond(), otherPair.getSecond())) ||
                        (!this.isOrdered() && Objects.equals(this.getFirst(), otherPair.getSecond()) && Objects.equals(this.getSecond(), otherPair.getFirst())));
    }

    @Override
    public int hashCode() {
        return this.isOrdered() ?
                getFirst().hashCode() ^ (getSecond().hashCode() << 16) :
                getFirst().hashCode() ^ getSecond().hashCode();
    }
}
