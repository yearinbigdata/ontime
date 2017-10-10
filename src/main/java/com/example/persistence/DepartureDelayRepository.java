package com.example.persistence;

import org.springframework.data.mongodb.repository.MongoRepository;
import com.example.domain.DepartureDelay;

public interface DepartureDelayRepository extends MongoRepository<DepartureDelay, String> {

}





