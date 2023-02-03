
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    Client,
    Region as Region};
//use aws_types::region::Region;


pub const REGION: &str = "eu-central-1";
//pub const REGION: &str = "us-west-2";


// the the current region and a client for this region
pub async fn get_region_client() -> (Region, Client) {
//    let region_provider = RegionProviderChain::first_try(Region::new(REGION));
    let region_provider = RegionProviderChain::first_try(REGION);
    let region = region_provider.region().await.unwrap();

    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&shared_config);

    (region, client)
}

/// get a client for the current project
pub async fn get_client() -> Client {
    let (_, client) = get_region_client().await;

    client
}
