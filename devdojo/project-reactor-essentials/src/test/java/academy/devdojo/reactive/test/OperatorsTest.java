package academy.devdojo.reactive.test;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class OperatorsTest {

    @BeforeAll
    public static void setup() {
        BlockHound.install(builder ->
                builder.allowBlockingCallsInside("org.slf4j.impl.SimpleLogger", "write"));
    }

    @Test
    public void subscribeOnSimple() {

        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void publishOnSimple() {

        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void multiplesSubscribeOnSimple() {

        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void multiplesPublishOnSimple() {

        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void publishAndSubscribeOn() {

        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void subscribeAndPublishOn() {

        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();

    }

    @Test
    public void subscribeOnIO() throws InterruptedException {

        Mono<List<String>> fileMono = Mono.fromCallable(() -> Files.readAllLines(Path.of("file.txt")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(fileMono)
                .expectSubscription()
                .thenConsumeWhile(list -> {
                    assertFalse(list.isEmpty());
                    log.info("Size {}", list.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void switchIfEmptyOperator() {

        Flux<Object> flux = Flux.empty()
                .switchIfEmpty(Flux.just("Not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("Not empty anymore")
                .expectComplete()
                .verify();

    }

    @Test
    public void deferOperator() throws Exception {

        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(t -> log.info("time {}", t));
        Thread.sleep(100);
        defer.subscribe(t -> log.info("time {}", t));
        Thread.sleep(100);
        defer.subscribe(t -> log.info("time {}", t));
        Thread.sleep(100);
        defer.subscribe(t -> log.info("time {}", t));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);

        assertTrue(atomicLong.get() > 0);

    }

    @Test
    public void concatOperator() {

        Flux<String> first = Flux.just("a", "b");
        Flux<String> second = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(first, second)
                .log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .verifyComplete();

    }

    @Test
    public void concatWithOperator() {

        Flux<String> first = Flux.just("a", "b");
        Flux<String> second = Flux.just("c", "d");

        Flux<String> concatFlux = first.concatWith(second)
                .log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .verifyComplete();

    }

    @Test
    public void concatOperatorError() {

        Flux<String> first = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> second = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concatDelayError(first, second)
                .log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError()
                .verify();

    }

    @Test
    public void combineLastOperator() {

        Flux<String> first = Flux.just("a", "b");//.delayElements(Duration.ofSeconds(1))
        Flux<String> second = Flux.just("c", "d");

        Flux<String> combineLatest = Flux.combineLatest(first, second, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                .log();

        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("BC", "BD")
                .verifyComplete();

    }

    @Test
    public void mergeOperator() throws InterruptedException {
        Flux<String> first = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> second = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.merge(first, second)
                .delayElements(Duration.ofMillis(200))
                .log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeWithOperator() throws InterruptedException {
        Flux<String> first = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> second = Flux.just("c", "d");

        Flux<String> mergeFlux = first.mergeWith(second)
                .delayElements(Duration.ofMillis(200))
                .log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeSequencialOperator() throws InterruptedException {
        Flux<String> first = Flux.just("a", "b").delayElements(Duration.ofMillis(100));
        Flux<String> second = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.mergeSequential(first, second, first)
                .delayElements(Duration.ofMillis(200))
                .log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeDelayErrorOperator() throws InterruptedException {
        Flux<String> first = Flux.just("a", "b")
                .map(s -> {
                    if (s.equals("b")) {
                        throw new IllegalArgumentException();
                    }
                    return s;
                });
        Flux<String> second = Flux.just("c", "d");

        Flux<String> mergeFlux = Flux.mergeDelayError(1, first, second)
                .log();

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("a", "c", "d")
                .expectError()
                .verify();

    }

    @Test
    public void flatMapOperator() throws Exception {

        Flux<String> flux = Flux.just("a", "b");

        Flux<String> names = flux.map(String::toUpperCase)
                .flatMap(this::findByName)
                .log();

        StepVerifier.create(names)
                .expectNext("nameB1", "nameB2", "nameA1", "nameA2")
                .expectComplete()
                .verify();

    }

    @Test
    public void flatMapSequencialOperator() throws Exception {

        Flux<String> flux = Flux.just("a", "b");

        Flux<String> names = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName)
                .log();

        StepVerifier.create(names)
                .expectNext("nameA1", "nameA2", "nameB1", "nameB2")
                .expectComplete()
                .verify();

    }

    private Flux<String> findByName(String name) {
        return "A".equals(name) ? Flux.just("nameA1", "nameA2").delayElements(Duration.ofMillis(100)) : Flux.just("nameB1", "nameB2");
    }

    @Test
    public void zipOperator() {

        Flux<String> titles = Flux.just("Grand Blue", "Shingeki no Kyojin");
        Flux<String> studios = Flux.just("Zero-G", "Mappa");
        Flux<Integer> episodes = Flux.just(12, 100);

        Flux<Anime> animes = Flux.zip(titles, studios, episodes)
                .map(t -> new Anime(t.getT1(), t.getT2(), t.getT3()))
                .log();

        StepVerifier.create(animes)
                .expectSubscription()
                .expectNext(
                        new Anime("Grand Blue", "Zero-G", 12),
                        new Anime("Shingeki no Kyojin", "Mappa", 100)
                )
                .expectComplete()
                .verify();

    }

    @Test
    public void zipWithOperator() {

        Flux<String> titles = Flux.just("Grand Blue", "Shingeki no Kyojin");
        Flux<String> studios = Flux.just("Zero-G", "Mappa");
        Flux<Integer> episodes = Flux.just(12, 100);

        Flux<Anime> animes = titles
                .zipWith(studios)
                .zipWith(episodes)
                .map(t -> new Anime(t.getT1().getT1(), t.getT1().getT2(), t.getT2()))
                .log();

        StepVerifier.create(animes)
                .expectSubscription()
                .expectNext(
                        new Anime("Grand Blue", "Zero-G", 12),
                        new Anime("Shingeki no Kyojin", "Mappa", 100)
                )
                .expectComplete()
                .verify();

    }

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    class Anime {
        private String title;
        private String studio;
        private int episodes;
    }

}
