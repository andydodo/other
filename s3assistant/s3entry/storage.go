package s3entry

const (
	S3_RETRY_SLEEP_SECONDS = 10
	S3_RETRY_TIMES         = 3
	EMPTY_STRING           = ""
)

const (
	CREATE_MULTIPART_UPLOAD_FUNCTION    = "CreateMultipartUpload"
	GET_OBJECT_FUNCTION                 = "GetObject"
	DELETE_OBJECT_FUNCTION              = "DeleteObject"
	UPLOAD_PART_FUNCTION                = "UploadPart"
	PUT_OBJECT_FUNCTION                 = "PutObject"
	COMPLETED_MULTIPART_UPLOAD_FUNCTION = "CompletedMultipartUpload"
	ABORT_MULTIUPLOAD_FUNCTION          = "AbortMultiUpload"
	OBJECT_CHANGED_FUNCTION             = "OBJECT_CHANGED_FUNCTION"
)
