package json

type ObjectInfo struct {
	BucketName         string        `json:"bucket_name"`
	ObjectName         string        `json:"object_name"`
	Etag               string        `json:"etag"`
	Size               int64         `json:"size"`
	Owner              string        `json:"owner"`
	LastModified       int64         `json:"last_modified"`
	StorageClass       int64         `json:"storage_class"`
	ACL                string        `json:"acl"`
	UUID               string        `json:"uuid"`
	Location           string        `json:"location"`
	UploadID           string        `json:"upload_id"`
	IndexType          int64         `json:"index_type"`
	Objectparts        []interface{} `json:"objectparts"`
	ObjectpartGroupIDS []interface{} `json:"objectpart_group_ids"`
	Blocks             []Block       `json:"blocks"`
	XAmzMeta           []interface{} `json:"-"`
}

type Block struct {
	Key    string `json:"key"`
	Offset int64  `json:"offset"`
	Len    int64  `json:"len"`
}
