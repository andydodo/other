package s3client

type S3Client struct {
	S3Bucket    string //Bucket名称，从hulk可以获取
	S3AccessKey string //AccessKey名称，从hulk可以获取
	S3SecretKey string //SecretKey名称，从hulk可以获取
	S3Region    string
	S3EndPoint  string //S3的域名，从hulk可以获取
}

func NewS3Client() *S3Client {
	return &S3Client{
		S3Bucket:    "",
		S3AccessKey: "",
		S3SecretKey: "",
		S3Region:    "default",
		S3EndPoint:  "",
	}
}
