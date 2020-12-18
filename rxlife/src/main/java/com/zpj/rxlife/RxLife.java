package com.zpj.rxlife;

import android.app.Activity;
import android.arch.lifecycle.Lifecycle;
import android.arch.lifecycle.LifecycleObserver;
import android.arch.lifecycle.LifecycleOwner;
import android.arch.lifecycle.LifecycleRegistry;
import android.arch.lifecycle.OnLifecycleEvent;
import android.content.Context;
import android.support.annotation.MainThread;
import android.view.View;

import java.util.HashMap;

import io.reactivex.Observable;
import io.reactivex.annotations.NonNull;
import io.reactivex.functions.Predicate;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.PublishSubject;

public class RxLife {

    private static final HashMap<LifecycleOwner, RxLifecycleOwner> TAG_LIFECYCLE_MAP =
            new HashMap<>();

    /**
     * 处理tag 发送事件形式的绑定处理
     */
    private static final PublishSubject<String> TAG_EVENT_SUBJECT = PublishSubject.create();

    public static <T> LifecycleTransformer<T> bindTag(final String tag) {
        return bindTag(tag, true);
    }

    public static <T> LifecycleTransformer<T> bindTag(final String tag, boolean disposeBefore) {
        if (tag == null) {
            return bindError(
                    new NullPointerException("RxLifeHelper: parameter tag can not be null"));
        }
        if (disposeBefore) {
            removeTag(tag);
        }
        return bind(TAG_EVENT_SUBJECT.filter(new Predicate<String>() {
            @Override
            public boolean test(@NonNull String innerTag) throws Exception {
                return tag.equals(innerTag);
            }
        }));
    }

    public static void removeTag(String tag) {
        TAG_EVENT_SUBJECT.onNext(tag);
    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindView(final View view) {
        if (view == null) {
            return bindNullError("view could not be null");
        }
        AttachListener listener = (AttachListener) view.getTag(R.id._rxlife_tag_view_attach);
        if (listener == null) {
            synchronized (RxLife.class) {
                listener = (AttachListener) view.getTag(R.id._rxlife_tag_view_attach);
                if (listener == null) {
                    listener = new AttachListener();
                    view.addOnAttachStateChangeListener(listener);
                    view.setTag(R.id._rxlife_tag_view_attach, listener);
                }
            }
        }
        return bind(listener.lifecycleSubject);
    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindRootView(final View view) {
        return bindView(view.getRootView());
    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindActivity(final Activity activity) {
        if (activity instanceof LifecycleOwner) {
            return bindLifeOwner((LifecycleOwner) activity, Lifecycle.Event.ON_DESTROY);
        }
        if (activity == null || activity.getWindow() == null || activity.isFinishing()) {
            return bindError(new IllegalStateException("activity status not good"));
        }
        return bindView(activity.getWindow().getDecorView());
    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindContext(Context target,
                                                                 Lifecycle.Event event) {
        if (!(target instanceof LifecycleOwner)) {
            return bindError(
                    new IllegalArgumentException("RxLifeHelper: target must implements LifecycleOwner"));
        }
        return bindLifeOwner((LifecycleOwner) target, event);
    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindLifeOwner(LifecycleOwner lifecycleOwner) {
        return bindLifeOwner(lifecycleOwner, Lifecycle.Event.ON_DESTROY);
    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindLifeOwner(LifecycleOwner lifecycleOwner,
                                                                      Lifecycle.Event event) {
        if (lifecycleOwner == null) {
            return bindNullError("RxLifeHelper: target could not be null");
        }
        if (lifecycleOwner.getLifecycle() == null) {
            return bindNullError("RxLifeHelper: lifecycle could not be null");
        }
        if (lifecycleOwner.getLifecycle().getCurrentState() == Lifecycle.State.DESTROYED) {
            return bindNullError("RxLifeHelper: LifecycleOwner was destroyed");
        }
        return bindUntilEvent(getLifeManager(lifecycleOwner).lifecycleSubject, event);
    }

    ///////////////////////////////////// live data ///////////////////////////////////////////
    //              配合liveData onNext、onSuccess等回调，会强制回到主线程                       //
    //         use with liveData onNext、onSuccess .etc will call on UI thread               //
    ///////////////////////////////////////////////////////////////////////////////////////////

    @MainThread
    public static <T> LifecycleTransformer<T> bindContextLive(Context context,
                                                                     Lifecycle.Event event) {
        if (context instanceof LifecycleOwner) {
            return bindLifeOwnerLive((LifecycleOwner) context, event);

        } else if (context instanceof Activity) {
            return bindActivity((Activity) context);
        }
        return bindError(
                new IllegalArgumentException("RxLifeHelper: target must implements LifecycleOwner or Activity"));

    }

    @MainThread
    public static <T> LifecycleTransformer<T> bindLifeOwnerLive(
            LifecycleOwner lifecycleOwner, Lifecycle.Event event) {
        if (lifecycleOwner == null) {
            return bindNullError("RxLifeHelper: target could not be null");
        }
        if (lifecycleOwner.getLifecycle() == null) {
            return bindNullError("RxLifeHelper: lifecycle could not be null");
        }
        if (lifecycleOwner.getLifecycle().getCurrentState() == Lifecycle.State.DESTROYED) {
            return bindNullError("RxLifeHelper: LifecycleOwner was destroyed");
        }
        RxLife.RxLifecycleOwner lifeCycleManager = getLifeManager(lifecycleOwner);
        return bindUntilEvent(lifeCycleManager, lifeCycleManager.lifecycleSubject, event);
    }

    private static RxLifecycleOwner getLifeManager(@NonNull final LifecycleOwner lifecycleOwner) {
        RxLifecycleOwner lifeCycleManager = TAG_LIFECYCLE_MAP.get(lifecycleOwner);
        if (lifeCycleManager == null) {
            synchronized (TAG_LIFECYCLE_MAP) {
                lifeCycleManager = TAG_LIFECYCLE_MAP.get(lifecycleOwner);
                if (lifeCycleManager == null) {
                    lifeCycleManager = new RxLifecycleOwner(lifecycleOwner);
                    lifecycleOwner.getLifecycle().addObserver(lifeCycleManager);
                    TAG_LIFECYCLE_MAP.put(lifecycleOwner, lifeCycleManager);
                }
            }
        }
        return lifeCycleManager;
    }

    private static <T> LifecycleTransformer<T> bindNullError(String msg) {
        return bind(Observable.error(new NullPointerException(msg)));
    }

    private static <T> LifecycleTransformer<T> bindError(Throwable throwable) {
        return bind(Observable.error(throwable));
    }

    private static <T, R> LifecycleTransformer<T> bindUntilEvent(final Observable<R> lifecycle,
                                                         final R event) {
        return bind(takeUntilEvent(lifecycle, event));
    }

    private static <T, R> LifecycleTransformer<T> bindUntilEvent(LifecycleOwner lifecycleOwner,
                                                         final Observable<R> lifecycle, final R event) {
        return new LifeDataTransformer<>(lifecycleOwner, takeUntilEvent(lifecycle, event));
    }

    private static <R> Observable<R> takeUntilEvent(final Observable<R> lifecycle, final R event) {
        return lifecycle.filter(new Predicate<R>() {
            @Override
            public boolean test(@NonNull R lifecycleEvent) throws Exception {
                return lifecycleEvent.equals(event);
            }
        });
    }

    private static <T, R> LifecycleTransformer<T> bind(final Observable<R> lifecycle) {
        return new LifecycleTransformer<>(lifecycle);
    }

    private static class RxLifecycleOwner implements LifecycleOwner, LifecycleObserver {

        private final BehaviorSubject<Lifecycle.Event> lifecycleSubject = BehaviorSubject.create();
        private final LifecycleOwner source;
        private LifecycleRegistry registry;

        private RxLifecycleOwner(LifecycleOwner source) {
            this.source = source;
            this.registry = new LifecycleRegistry(source);
        }

        @OnLifecycleEvent(Lifecycle.Event.ON_CREATE)
        void onCreate() {
            onStateChanged(source, Lifecycle.Event.ON_CREATE);
        }

        @OnLifecycleEvent(Lifecycle.Event.ON_START)
        void onStart() {
            onStateChanged(source, Lifecycle.Event.ON_START);
        }

        @OnLifecycleEvent(Lifecycle.Event.ON_RESUME)
        void onResume() {
            onStateChanged(source, Lifecycle.Event.ON_RESUME);
        }

        @OnLifecycleEvent(Lifecycle.Event.ON_PAUSE)
        void onPause() {
            onStateChanged(source, Lifecycle.Event.ON_PAUSE);
        }

        @OnLifecycleEvent(Lifecycle.Event.ON_STOP)
        void onStop() {
            onStateChanged(source, Lifecycle.Event.ON_STOP);
        }

        @OnLifecycleEvent(Lifecycle.Event.ON_DESTROY)
        void onDestroy() {
            onStateChanged(source, Lifecycle.Event.ON_DESTROY);
        }

        @NonNull
        @Override
        public Lifecycle getLifecycle() {
            return registry;
        }

        private void onStateChanged(final LifecycleOwner source, Lifecycle.Event event) {
            registry.handleLifecycleEvent(event);
            lifecycleSubject.onNext(event);
            if (event == Lifecycle.Event.ON_DESTROY) {
                source.getLifecycle().removeObserver(this);
                TAG_LIFECYCLE_MAP.remove(source);
                registry = null;
            }
        }

    }

    private static class AttachListener implements View.OnAttachStateChangeListener {

        private final PublishSubject<Boolean> lifecycleSubject = PublishSubject.create();

        @Override
        public void onViewAttachedToWindow(View v) {
        }

        @Override
        public void onViewDetachedFromWindow(View v) {
            v.removeOnAttachStateChangeListener(this);
            lifecycleSubject.onNext(true);
            v.setTag(R.id._rxlife_tag_view_attach, null);
        }
    }


}

