
use std::future::Future;
use futures::executor::block_on;
use tokio;


/// Check if an executor is available and run action on this executor, otherwise start a runtime to run and block_on the async action
pub fn run_async<F>(action: F) -> F::Output 
where F: Future { 
// where F: Future + Send + 'static, 
//       F::Output: 'static + Send {

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
            println!("TMP: ========>   RUN ASYNC");
            handle.block_on(action)
        }
        // No async runtime, so create one and launch this task on it 
        // TODO: check duration of lanuching a runtime
        Err(err) => {
            println!("No runtime running. When retrieving runtime got {err:?}\nStart temporary (multi-threaded) runtime now.");
            tokio::runtime::Runtime::new()
            // tokio::runtime::Builder::new_current_thread()
            // .enable_all()
            // .build()
            .unwrap()
            .block_on(action)
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
            println!("No runtime running. When retrieving runtime got {err:?}\nStart temporary (multi-threaded) runtime now.");
            tokio::runtime::Runtime::new()
            // tokio::runtime::Builder::new_current_thread()
            // .enable_all()
            // .build()
            .unwrap()
            .spawn(action)
        }
    }
}