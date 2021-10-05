Library Kafka Producer and Consumer sample project

# Run application using docker
Run your containers:

```$ docker-compose up OR $ docker-compose up --build --force-recreate```

Then run your producer and consumer applications.

import the ```LibraryMicroservices.postman_collection.json``` to your postman
then you can use any of the endpoints. The main one I used to make the tests is:

```POST/PUT localhost:8080/v1/library-event-with-topic-and-header```

the body is in the postman collection.

Project based on this course
https://www.udemy.com/course/apache-kafka-for-developers-using-springboot
