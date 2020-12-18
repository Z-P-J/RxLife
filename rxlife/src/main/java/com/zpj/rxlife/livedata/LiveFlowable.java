package com.zpj.rxlife.livedata;

import android.arch.lifecycle.LifecycleOwner;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.EndConsumerHelper;

/**
 * @author genius
 * @date 2019/11/3
 */
public final class LiveFlowable<T> extends Flowable<T> {

    private final Flowable<T> upstream;
    private final LifecycleOwner lifecycleOwner;

    public LiveFlowable(Flowable<T> upstream, LifecycleOwner lifecycleOwner) {
        this.upstream = upstream;
        this.lifecycleOwner = lifecycleOwner;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> observer) {
        upstream.subscribe(new LiveObserver<>(lifecycleOwner, observer));
    }

    static class LiveObserver<T> extends AbsLiveDataObserver<T>
            implements Subscriber<T>, Subscription, Disposable {
        private final Subscriber<? super T> downstream;
        private final LifecycleOwner lifecycleOwner;
        private final AtomicReference<Subscription> upstream = new AtomicReference<>();

        LiveObserver(LifecycleOwner lifecycleOwner, final Subscriber<? super T> downstream) {
            super(lifecycleOwner);
            this.downstream = downstream;
            this.lifecycleOwner = lifecycleOwner;
        }

        @Override
        public void onSubscribe(Subscription s) {
            EndConsumerHelper.setOnce(this.upstream, s, getClass());
            downstream.onSubscribe(this);
        }

        @Override
        public void onNext(T data) {
            onLiveNext(data);
        }

        @Override
        public void onError(Throwable e) {
            removeObservers(lifecycleOwner);
            downstream.onError(e);
        }

        @Override
        public void onComplete() {
            removeObservers(lifecycleOwner);
            downstream.onComplete();
        }

        @Override
        public void request(long n) {
            upstream.get().request(n);
        }

        @Override
        public void cancel() {
            dispose();
        }

        @Override
        public void onChanged(T data) {
            downstream.onNext(data);
        }

        @Override
        public final boolean isDisposed() {
            return upstream.get() == SubscriptionHelper.CANCELLED;
        }

        @Override
        public final void dispose() {
            removeObservers(lifecycleOwner);
            SubscriptionHelper.cancel(upstream);
        }
    }
}
