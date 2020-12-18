package com.zpj.rxlife.livedata;

import android.arch.lifecycle.LifecycleOwner;

import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.disposables.DisposableHelper;

/**
 * @author genius
 * @date 2019/11/3
 */
public final class LiveSingle<T> extends Single<T> {
    private final Single<T> upstream;
    private final LifecycleOwner lifecycleOwner;

    public LiveSingle(Single<T> upstream, LifecycleOwner lifecycleOwner) {
        this.upstream = upstream;
        this.lifecycleOwner = lifecycleOwner;
    }

    @Override
    protected void subscribeActual(@NonNull SingleObserver<? super T> observer) {
        upstream.subscribe(new LiveObserver<>(lifecycleOwner, observer));
    }

    static class LiveObserver<T> extends AbsLiveDataObserver<T>
            implements SingleObserver<T>, Disposable {
        private final SingleObserver<? super T> downstream;
        private final LifecycleOwner lifecycleOwner;
        private final AtomicReference<Disposable> upstream = new AtomicReference<>();

        LiveObserver(final LifecycleOwner lifecycleOwner, final SingleObserver<? super T> downstream) {
            super(lifecycleOwner);
            this.downstream = downstream;
            this.lifecycleOwner = lifecycleOwner;
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            DisposableHelper.setOnce(this.upstream, d);
            downstream.onSubscribe(this);
        }

        @Override
        public void onSuccess(@NonNull T data) {
            onLiveNext(data);
        }

        @Override
        public void onError(@NonNull Throwable e) {
            removeObservers(lifecycleOwner);
            downstream.onError(e);
        }

        @Override
        public void dispose() {
            removeObservers(lifecycleOwner);
            DisposableHelper.dispose(upstream);
        }

        @Override
        public final boolean isDisposed() {
            return upstream.get() == DisposableHelper.DISPOSED;
        }

        @Override
        public void onChanged(T data) {
            removeObservers(lifecycleOwner);
            downstream.onSuccess(data);
        }
    }
}
