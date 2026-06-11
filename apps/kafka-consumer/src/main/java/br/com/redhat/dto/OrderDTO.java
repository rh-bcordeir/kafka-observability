package br.com.redhat.dto;

public record OrderDTO(
    String key,
    String title,
    String description,
    Integer size
){}
