package com.zpj.rxlife.demo;

import android.arch.lifecycle.Lifecycle;
import android.os.Bundle;
import android.os.Looper;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import com.zpj.rxlife.RxLife;

import org.reactivestreams.Subscription;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.Observer;
import io.reactivex.Single;
import io.reactivex.android.schedulers.AndroidSchedulers;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;

public class MainActivity extends AppCompatActivity {

    private static final String TAG = "MainActivity";

    private TextView tvText;
    private Button btnInit;
    private ViewGroup view;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        view = (ViewGroup) LayoutInflater.from(this).inflate(R.layout.activity_main, null, false);
        setContentView(view);

        tvText = findViewById(R.id.tv_text);
        btnInit = findViewById(R.id.btn_init);

        findViewById(R.id.btn_remove_tag).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Toast.makeText(MainActivity.this, "移除Tag:" + TAG, Toast.LENGTH_SHORT).show();
                RxLife.removeByTag(MainActivity.this);
                btnInit.setVisibility(View.VISIBLE);
            }
        });

        findViewById(R.id.btn_remove_view).setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Toast.makeText(MainActivity.this, "移除View:" + tvText, Toast.LENGTH_SHORT).show();
                if (view != null) {
                    view.removeView(tvText);
                    btnInit.setVisibility(View.VISIBLE);
                }
            }
        });

        btnInit.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Toast.makeText(MainActivity.this, "初始化", Toast.LENGTH_SHORT).show();
                init();
            }
        });

//        init();

    }

    private void init() {
        tvText.setText("Hello World!");
        btnInit.setVisibility(View.GONE);
        if (tvText.getParent() == null) {
            view.addView(tvText);
        }
        Observable.timer(10000, TimeUnit.MILLISECONDS)
//                .observeOn(Schedulers.io())
                .observeOn(AndroidSchedulers.mainThread())
                .compose(RxLife.<Long>bindTag(this))
                //配合生命周期
                .compose(RxLife.<Long>bindLifeOwner(this, Lifecycle.Event.ON_PAUSE))
                // 在配合生命周期的前提下，配合LiveData
//                .compose(RxLife.<Long>bindLifeOwnerLive(this, Lifecycle.Event.ON_PAUSE))
                .compose(RxLife.<Long>bindView(tvText))
                .subscribe(new Observer<Long>() {
                    @Override
                    public void onSubscribe(@NonNull Disposable d) {
                        Log.e(TAG, "onSubscribe isMain=" + isMain());
                    }

                    @Override
                    public void onNext(@NonNull Long data) {
                        Toast.makeText(MainActivity.this, "onNext data=" + data, Toast.LENGTH_SHORT).show();
                        Log.e(TAG, "event " + data + " isMain=" + isMain());
                    }

                    @Override
                    public void onError(@NonNull Throwable e) {
                        e.printStackTrace();
                        Log.e(TAG, "onError isMain=" + isMain());
                    }

                    @Override
                    public void onComplete() {
                        Toast.makeText(MainActivity.this, "onComplete", Toast.LENGTH_SHORT).show();
                        Log.e(TAG, "doOnComplete isMain=" + isMain());
                        tvText.setText("onComplete");
                    }
                });
    }

    @Override
    protected void onPause() {
        super.onPause();
        Toast.makeText(this, "onPause", Toast.LENGTH_SHORT).show();
    }

    @Override
    protected void onResume() {
        super.onResume();

        init();

        final AtomicInteger atomicInteger = new AtomicInteger();

        final long start = System.currentTimeMillis();

        //Flowable.create(new FlowableOnSubscribe<Object>() {
        //  @Override public void subscribe(final FlowableEmitter<Object> emitter) throws Exception {
        //    Single.timer(2000, TimeUnit.MILLISECONDS).subscribe(new BiConsumer<Long, Throwable>() {
        //      @Override public void accept(Long aLong, Throwable throwable) throws Exception {
        //        emitter.onError(new Exception("E"));
        //        Log.e("onNext", " " + "E");
        //      }
        //    });
        //    Observable.interval(200, TimeUnit.MILLISECONDS).subscribe(new Consumer<Long>() {
        //      @Override public void accept(Long aLong) throws Exception {
        //        emitter.onNext(aLong);
        //      }
        //    });
        //  }
        //}, BackpressureStrategy.BUFFER)
        //    .compose(RxLifeHelper.bindUntilLifeLiveEvent(this, Lifecycle.Event.ON_DESTROY))
        //    .subscribe(new FlowableSubscriber<Object>() {
        //  @Override public void onSubscribe(Subscription s) {
        //    s.request(Integer.MAX_VALUE);
        //  }
        //
        //  @Override public void onNext(Object o) {
        //    Log.e("onNext", "" + o);
        //  }
        //
        //  @Override public void onError(Throwable t) {
        //
        //  }
        //
        //  @Override public void onComplete() {
        //
        //  }
        //});

        // 1111111111111 将不会 被打印
        //getData("111111111111111111111111111111");
        //getData("222222222222222222222222222222");

        //for (int i = 0; i < 1; i++) {
        //  getSupportFragmentManager().beginTransaction()
        //      .add(R.id.fl_fragment, new MainFragment(), i + "")
        //      .commit();
        //}
    }

    private void getData(final String data) {
        Flowable.timer(2000, TimeUnit.MILLISECONDS).subscribe(new FlowableSubscriber<Long>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Integer.MAX_VALUE);
                Log.e("getDatagetData", "onSubscribe ");
            }

            @Override
            public void onNext(Long aLong) {
                Log.e("getDatagetData", "onNextonNext " + aLong);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onComplete() {

            }
        });
        Single.timer(1000, TimeUnit.MILLISECONDS)
                .compose(RxLife.<Long>bindTag("getData"))
                .compose(RxLife.<Long>bindLifeOwner(this, Lifecycle.Event.ON_PAUSE))
                .doOnDispose(new Action() {
                    @Override
                    public void run() throws Exception {
                        Log.e("doOnDispose", "run: doOnDisposedoOnDispose");
                    }
                })
                .subscribe(new Consumer<Long>() {
                    @Override
                    public void accept(Long aLong) throws Exception {
                        Log.e("RxLifeHelper", "event " + data);
                    }
                }, new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable throwable) throws Exception {
                        Log.e("getData", "accept: " + throwable);
                    }
                });
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();

        final int[] num = new int[]{0};
        Observable.interval(200, TimeUnit.MILLISECONDS)
                .flatMap(new Function<Long, ObservableSource<?>>() {
                    @Override
                    public ObservableSource<?> apply(Long aLong) throws Exception {
                        if (num[0]++ > 20) {
                            return Observable.error(new Exception("wewe"));
                        }
                        return Observable.just(num[0]);
                    }
                })
                .compose(RxLife.bindLifeOwner(this))
                .subscribe(new Consumer<Object>() {
                    @Override
                    public void accept(Object o) throws Exception {
                        Log.e("onNext", "" + o);
                    }
                }, new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable throwable) throws Exception {
                        Log.e("onNext", "" + throwable.getMessage());
                    }
                });

    }


    private boolean isMain() {
        return Looper.getMainLooper().getThread() == Thread.currentThread();
    }

}
