package academy.devdojo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {

    @BeforeAll
    public static void setup() {
        BlockHound.install();
    }

    @Test
    public void fluxSubscriber() {

        Flux<String> flux = Flux.just("Caique", "Borges")
                .log();

        StepVerifier.create(flux)
                .expectNext("Caique", "Borges")
                .verifyComplete();
    }

    @Test
    public void fluxNumbers() {
        Flux<Integer> fluxNumbers = Flux.range(1, 5)
                .log();

        fluxNumbers.subscribe(i -> log.info("Number {}", i));

        log.info("--------------------------------------------");

        StepVerifier.create(fluxNumbers)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    public void fluxNumbersFromList() {
        Flux<Integer> fluxNumbers = Flux.fromIterable(List.of(1, 2, 3, 4, 5))
                .log();

        fluxNumbers.subscribe(i -> log.info("Number {}", i));

        log.info("--------------------------------------------");

        StepVerifier.create(fluxNumbers)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError() {
        Flux<Integer> fluxNumbers = Flux.range(1, 5)
                .log()
                .map(i -> {
                    if (i == 4) throw new IndexOutOfBoundsException("index error");
                    return i;
                });

        fluxNumbers.subscribe(i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("DONE!"),
                subscription -> subscription.request(3));

        log.info("--------------------------------------------");

        StepVerifier.create(fluxNumbers)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersUglyBackpressure() {
        Flux<Integer> fluxNumbers = Flux.range(1, 10)
                .log();

        fluxNumbers.subscribe(new Subscriber<Integer>() {

            private int count = 0;
            private Subscription subscription;
            private int requestCount = 2;

            @Override
            public void onSubscribe(Subscription s) {
                this.subscription = s;
                subscription.request(2);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    subscription.request(requestCount);
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });

        log.info("--------------------------------------------");

        StepVerifier.create(fluxNumbers)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersNotSoUglyBackpressure() {
        Flux<Integer> fluxNumbers = Flux.range(1, 10)
                .log();

        fluxNumbers.subscribe(new BaseSubscriber<Integer>() {

            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });

        log.info("--------------------------------------------");

        StepVerifier.create(fluxNumbers)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void fluxNumbersPrettyBackPressure() {
        Flux<Integer> fluxNumbers = Flux.range(1, 10)
                .log()
                .limitRate(3);

        fluxNumbers.subscribe(i -> log.info("Number {}", i));

        log.info("--------------------------------------------");

        StepVerifier.create(fluxNumbers)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }


    @Test
    public void fluxSubscriberInterval() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(10)
                .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    public void fluxSubscriberIntervalWithVirtualTime() throws InterruptedException {

        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofDays(1))
                .take(10)
                .log())
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L)
                .expectNext(1L)
                .thenCancel()
                .verify();

        StepVerifier.withVirtualTime(() -> Flux.interval(Duration.ofDays(1))
                .take(10)
                .log())
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();

    }

    @Test
    public void connectableFlux_HotFlux() throws InterruptedException {

        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        /*
        connectableFlux.connect();

        log.info("Thread sleeping for 300ms");

        Thread.sleep(300);

        connectableFlux.subscribe(i -> log.info("Sub1 number {}", i));

        log.info("Thread sleeping for 200ms");

        Thread.sleep(200);

        connectableFlux.subscribe(i -> log.info("Sub2 number {}", i));*/

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(i -> i <= 5)
                .expectNext(6, 7, 8, 9, 10)
                .expectComplete()
                .verify();
    }

    @Test
    public void connectableFlux_HotFlux_autoConnect() throws InterruptedException {

        Flux<Integer> fluxAutoConnect = Flux.range(1, 5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier
                .create(fluxAutoConnect)
                .then(fluxAutoConnect::subscribe)
                .expectNext(1, 2, 3, 4, 5)
                .expectComplete()
                .verify();
    }

}
