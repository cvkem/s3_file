
use std::future::Future;
use futures::executor::block_on;
use tokio;


/// Check if an executor is available and run action on this executor, otherwise start a runtime to run and block_on the async action
pub fn run_async<F>(action: F) -> F::Output 
    where F: Future {

    match tokio::runtime::Handle::try_current()  {
        // A runtime is running at the moment, so a block_on current executor is performed
        Ok(_) => block_on(action),
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