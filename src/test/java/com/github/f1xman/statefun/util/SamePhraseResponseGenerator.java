package com.github.f1xman.statefun.util;

public class SamePhraseResponseGenerator implements ResponseGenerator {
    @Override
    public String generateResponseTo(String phrase) {
        return phrase;
    }
}
