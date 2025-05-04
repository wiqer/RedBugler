package io.github.wiqer.local.key;

@FunctionalInterface
public interface ThreeParameterPredicate<T, U, V> {
    boolean hotKeyRule(T t, U u, V v);
}

