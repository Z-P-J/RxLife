/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zpj.rxlife;

import android.arch.lifecycle.LifecycleOwner;

import com.zpj.rxlife.livedata.LiveFlowable;
import com.zpj.rxlife.livedata.LiveMaybe;
import com.zpj.rxlife.livedata.LiveObservable;
import com.zpj.rxlife.livedata.LiveSingle;

import org.reactivestreams.Publisher;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Maybe;
import io.reactivex.MaybeSource;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.reactivex.annotations.NonNull;

/**
 * Transformer that continues a subscription until a second Observable emits an event.
 */
class LifeDataTransformer<T> extends LifecycleTransformer<T> {

    private final LifecycleOwner lifecycleOwner;

    LifeDataTransformer(LifecycleOwner lifecycleOwner, Observable<?> observable) {
        super(observable);
        this.lifecycleOwner = lifecycleOwner;
    }

    @NonNull
    @Override
    public ObservableSource<T> apply(final Observable<T> upstream) {
        return new LiveObservable<>(upstream, lifecycleOwner).takeUntil(observable);
    }

    @NonNull
    @Override
    public Publisher<T> apply(Flowable<T> upstream) {
        return new LiveFlowable<>(upstream, lifecycleOwner).takeUntil(
                observable.toFlowable(BackpressureStrategy.LATEST));
    }

    @NonNull
    @Override
    public SingleSource<T> apply(Single<T> upstream) {
        return new LiveSingle<>(upstream, lifecycleOwner).takeUntil(observable.firstOrError());
    }

    @NonNull
    @Override
    public MaybeSource<T> apply(Maybe<T> upstream) {
        return new LiveMaybe<>(upstream, lifecycleOwner).takeUntil(observable.firstElement());
    }
}
