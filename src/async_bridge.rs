
use std::{
    thread,
    future::Future};
//use futures::executor::block_on;
use tokio;

extern crate lazy_static;


lazy_static! {
    static ref RT: tokio::runtime::Runtime = (|| {
        let thread_name = "Async-bridge runtime"; 
        println!("Running a closure to create a runtime via Lazy_static!");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .thread_name("Async-bridge runtime")
            .build()
            .unwrap();

        rt.block_on(async {
            use console_subscriber;
            println!("Starting the console-subscriber for Tokio-console for thread '{}'", thread_name);
            console_subscriber::init();        
        });
        rt
    })();
//    static ref RT2: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

//  tokio::task::block_in_place is the alternative
// https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html
// in requires that you are in an async context to call this, or not ??



/// Check if an executor is available and run action on this executor, otherwise start a runtime to run and block_on the async action
pub fn run_async<F>(action: F) -> F::Output 
where F: Future { 
// where F: Future + Send + 'static, 
//       F::Output: 'static + Send {

    match tokio::runtime::Handle::try_current()  {
        //
        //  via one-shot channel
        // Ok(handle) => {
        //     //     println!("TMP: ========>   RUN ASYNC (via spawn and one-shot-channel to get return value");
        //     println!("TMP: ========>   RUN ASYNC (However async_bridge detected Runtime. Trying to spawn the future on it and await one-shot channel)");
        //     let (sender, mut receiver) = tokio::sync::oneshot::channel::<F::Output>();
        //     let handle = handle.clone();
        //     let jh = handle.spawn(async move {
        //         let result = action.await;
        //        sender.send(result);
        //     });
        //     // // await the result on the one-shot-channel
        //     // while !jh.is_finished() {
        //     //     tokio::time::sleep(tokio::time::Duration::from_millis(10));
        //     // }
        //    receiver.try_recv().unwrap()
        // },
        //
        // A runtime is running at the moment, so a block_on current executor is performed. This fails with error:
        //     thread 'tokio-runtime-worker' panicked at 'Cannot start a runtime from within a runtime. This happens because a function (like `block_on`) 
        //     attempted to block the current thread while the thread is being used to drive asynchronous tasks.', 
        // Ok(handle) => {
        //     let _guard = handle.enter();
        //     println!("TMP: ========>   RUN ASYNC");
        //     handle.block_on(action)
        // }
        //
        // run via handle is not a solution, see also: https://users.rust-lang.org/t/tokio-executor-block-on-freezing-and-handle-block-on-causing-cannot-start-a-runtime-from-within-a-runtime/63576
        // Ok(_handle) => {
        //     println!("TMP: ========>   RUN ASYNC (However async_bridge detected Runtime. Trying to reuse it)");
        //     // This version has the same issue:
        //     //   thread 'tokio-runtime-worker' panicked at 'Cannot start a runtime from within a runtime. This happens because a function (like `block_on`)
        //     //   attempted to block the current thread while the thread is being used to drive asynchronous tasks.
        //    //let handle = RT2.handle().clone();
        //    let handle = _handle.clone();
        //    let metrics = handle.metrics();
        //    println!("the Runtime metrics are: {metrics:?}");
        //    handle.block_on(action)
        //    // futures::executor::block_on(action)  //this simple executor blocks forever
        // }
        Ok(_handle) => {
//            println!("TMP: ========>   RUN ASYNC (However async_bridge detected Runtime. Trying to reuse it)");
            // This version has the same issue:
            //   thread 'tokio-runtime-worker' panicked at 'Cannot start a runtime from within a runtime. This happens because a function (like `block_on`)
            //   attempted to block the current thread while the thread is being used to drive asynchronous tasks.
           //let handle = RT2.handle().clone();
           let handle = _handle.clone();
           let metrics = handle.metrics();
           println!("the Runtime metrics are: {metrics:?}");
           tokio::task::block_in_place(move || handle.block_on(action))
           
           // futures::executor::block_on(action)  //this simple executor blocks forever
        }
        // No async runtime, so create one and launch this task on it 
        // TODO: check duration of lanuching a runtime
        Err(err) => {
//            println!("TMP: =====> RUN ASYNC:No async-runtime running. When retrieving runtime got {err:?}\nRun on local RT.");
            // tokio::runtime::Runtime::new()
            // // tokio::runtime::Builder::new_current_thread()
            // // .enable_all()
            // // .build()
            // .unwrap()
            // .block_on(action)
            RT.block_on(action)
        }
    }
}

/// Check if an executor is available and run action on this executor, otherwise start a runtime to run and block_on the async action
pub fn spawn_async<F>(action: F) -> tokio::task::JoinHandle<F::Output> 
where F: Future + Send + 'static,
       F::Output: 'static + Send {

    match tokio::runtime::Handle::try_current()  {
        // A runtime is running at the moment, so a block_on current executor is performed
        // Ok(handle) => {
        //    let (sender, mut receiver) = tokio::sync::oneshot::channel::<F::Output>();
        //     let jh = handle.spawn(async move {
        //         let result = action.await;
        //        sender.send(result);
        //     });
        //     // // await the result on the one-shot-channel
        //     // while !jh.is_finished() {
        //     //     tokio::time::sleep(tokio::time::Duration::from_millis(10));
        //     // }
        //    receiver.try_recv().unwrap()
        // },
        // Ok(_) => block_on(action),
        Ok(handle) => {
            let _guard = handle.enter();
            println!("TMP: ========>   SPAWN ASYNC");
            handle.spawn(action)
        }
        // No async runtime, so create one and launch this task on it 
        // TODO: check duration of lanuching a runtime
        Err(err) => {
            println!("TMP: =====> SPAWN ASYNC: No runtime running. When retrieving runtime got {err:?}\nStart temporary (multi-threaded) runtime now.");
            // tokio::runtime::Runtime::new()
            // // tokio::runtime::Builder::new_current_thread()
            // // .enable_all()
            // // .build()
            // .unwrap()
            // .spawn(action)
            RT.spawn(action)
        }
    }
}