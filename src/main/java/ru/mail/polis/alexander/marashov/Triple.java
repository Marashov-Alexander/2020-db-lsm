package ru.mail.polis.alexander.marashov;

/*
* Class with three template objects
*
* */
public class Triple<F, S, T> {

    public F first;
    public S second;
    public T third;

    public Triple(final F first, final S second, final T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
}
