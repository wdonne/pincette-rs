package net.pincette.rs;

import java.util.concurrent.CompletionStage;
import org.reactivestreams.Processor;

public interface AsyncProcessor<T> extends Processor<CompletionStage<T>, T> {}
