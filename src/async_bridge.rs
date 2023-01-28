
use std::future::Future;
use futures::executor::block_on;
use tokio;


/// Check if an executor is available and run action on this executor, otherwise start a runtime to run and block_on the async action
pub fn run_async<F>(action: F) -> F::Output 
    where F: Future {

    match tokio::runtime::Handle::try_current()  {
        Ok(_) => block_on(action),
        Err(err) => {
            println!("No runtime running. When retrieving runtime got {err:?}\nStart temporary runtime now.");
            tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(action)
        }
    }
}