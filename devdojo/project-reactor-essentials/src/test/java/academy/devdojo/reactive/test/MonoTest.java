package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

@Slf4j
/**
 * Reactive streams
 *  1. Asynchronous
 *  2. Non-blocking
 *  3. Backpressure
 *  Publisher <- (subscribe) Subscriber, Subscription is created
 *  Publisher (onSubscribe with the subscription) -> Subscriber
 *  Subscription <- (request N) Subscriber
 *  Publisher -> (onNext) Subscriber
 *      until any of:
 *      1. Publisher sends all the request objects.
 *      2. Publisher sends all the objects it has. (onComplete) subscriber and subscription will be canceled
 *      3. There is an error. (onError) -> subscriber and subscription will be canceled
 */
public class MonoTest {

    @BeforeAll
    public static void setup() {
        BlockHound.install();
    }

    @Test
    public void blockhoundworks() {

        try {
            FutureTask<String> futureTask = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });

            Schedulers.parallel()
                    .schedule(futureTask);

            futureTask.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }

    }

    @Test
    public void monoSubscriber() {

        final String name = "Caique Borges";
        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe();
        log.info("-------------------------");

        StepVerifier.create(mono)
                .expectNext("Caique Borges")
                .verifyComplete();

    }

    @Test
    public void monoSubscriberConsumer() {

        final String name = "Caique Borges";
        Mono<String> mono = Mono.just(name)
                .log();

        mono.subscribe(s -> log.info("value {}", s));
        log.info("-------------------------");

        StepVerifier.create(mono)
                .expectNext("Caique Borges")
                .verifyComplete();

    }

    @Test
    public void monoSubscriberConsumerError() {

        final String name = "Caique Borges";
        Mono<String> mono = Mono.just(name)
                .map(s -> {
                    throw new RuntimeException("testing mono with error");
                });

        mono.subscribe(
                s -> log.info("value {}", s),
                s -> log.error("something bad happened")
        );

        mono.subscribe(
                s -> log.info("value {}", s),
                Throwable::printStackTrace
        );

        log.info("-------------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();

    }

    @Test
    public void monoSubscriberConsumerComplete() {

        final String name = "Caique Borges";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));
        log.info("-------------------------");

        StepVerifier.create(mono)
                .expectNext("CAIQUE BORGES")
                .verifyComplete();

    }

    @Test
    public void monoSubscriberConsumerSubscription() {

        final String name = "Caique Borges";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);

        mono.subscribe(s -> log.info("value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"),
                Subscription::cancel);
        log.info("-------------------------");

        StepVerifier.create(mono)
                .expectNext("CAIQUE BORGES")
                .verifyComplete();

    }

    @Test
    public void monoDoOnMethods() {

        final String name = "Caique Borges";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed!"))
                .doOnRequest(longNumber -> log.info("Request received, starting doing somethig..."))
                .doOnNext(s -> log.info("Value is here, executing do on next {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value is here, executing do on next {}", s)) // will not be executed
                .doOnSuccess(s -> log.info("doOnSucess executed: {}", s));

        mono.subscribe(s -> log.info("value {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED!"));
        log.info("-------------------------");

        /*StepVerifier.create(mono)
                .expectNext("CAIQUE BORGES")
                .verifyComplete();*/

    }

    @Test
    public void monoDoOnErrorResume() {

        final String name = "Caique";

        Mono<Object> error = Mono.error(new IllegalArgumentException("mensagem!!s"))
                .onErrorResume(f -> {
                    log.info("inside on error resume");
                    return Mono.just(name);
                })
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectNext(name)
                .verifyComplete();

    }

    @Test
    public void monoDoOnErrorReturn() {

        final String name = "Caique";

        Mono<Object> error = Mono.error(new IllegalArgumentException("mensagem!!s"))
                .onErrorReturn("EMPTY")
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .log();

        StepVerifier.create(error)
                .expectNext("EMPTY")
                .verifyComplete();

    }
}
