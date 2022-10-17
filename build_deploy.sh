
# for instructions see:
#https://github.com/awslabs/aws-lambda-rust-runtime


cargo lambda build --release --arm64

cargo lambda deploy   --iam-role arn:aws:iam::085563133372:role/my_S3_exp

