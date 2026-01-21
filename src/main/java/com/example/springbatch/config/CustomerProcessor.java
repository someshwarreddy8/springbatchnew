package com.example.springbatch.config;

import com.example.springbatch.entity.Customer;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {
    @Override
    public @Nullable Customer process(Customer item) throws Exception {
        return item;
    }
}
