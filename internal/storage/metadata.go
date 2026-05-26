package storage

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketsBucket   = []byte("buckets")
	objectsBucket   = []byte("objects")
	multipartBucket = []byte("multipart")
)

// MetadataStore manages bucket and object metadata using bbolt.
type MetadataStore struct {
	db *bolt.DB
}

// NewMetadataStore opens or creates a bbolt database at the given path.
func NewMetadataStore(path string) (*MetadataStore, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata db: %w", err)
	}

	// Create top-level buckets
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketsBucket, objectsBucket, multipartBucket} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize metadata db: %w", err)
	}

	return &MetadataStore{db: db}, nil
}

// Close closes the metadata store.
func (m *MetadataStore) Close() error {
	return m.db.Close()
}

// PutBucket stores bucket metadata.
func (m *MetadataStore) PutBucket(info *BucketInfo) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(info.Name), data)
	})
}

// GetBucket retrieves bucket metadata by name.
func (m *MetadataStore) GetBucket(name string) (*BucketInfo, error) {
	var info BucketInfo
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(name))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", name)
		}
		return json.Unmarshal(data, &info)
	})
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// DeleteBucket removes bucket metadata.
func (m *MetadataStore) DeleteBucket(name string) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		return b.Delete([]byte(name))
	})
}

// ListBuckets returns all stored bucket metadata.
func (m *MetadataStore) ListBuckets() ([]BucketInfo, error) {
	var buckets []BucketInfo
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		return b.ForEach(func(k, v []byte) error {
			var info BucketInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			buckets = append(buckets, info)
			return nil
		})
	})
	return buckets, err
}

// BucketExists checks if a bucket exists in the metadata store.
func (m *MetadataStore) BucketExists(name string) (bool, error) {
	exists := false
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		exists = b.Get([]byte(name)) != nil
		return nil
	})
	return exists, err
}

// objectMetaKey returns the composite key for an object in the metadata store.
func objectMetaKey(bucket, key string) []byte {
	return []byte(bucket + "\x00" + key)
}

// PutObjectMeta stores object metadata.
func (m *MetadataStore) PutObjectMeta(info *ObjectInfo) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		data, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put(objectMetaKey(info.Bucket, info.Key), data)
	})
}

// GetObjectMeta retrieves object metadata.
func (m *MetadataStore) GetObjectMeta(bucket, key string) (*ObjectInfo, error) {
	var info ObjectInfo
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		data := b.Get(objectMetaKey(bucket, key))
		if data == nil {
			return fmt.Errorf("object not found: %s/%s", bucket, key)
		}
		return json.Unmarshal(data, &info)
	})
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// DeleteObjectMeta removes object metadata.
func (m *MetadataStore) DeleteObjectMeta(bucket, key string) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		return b.Delete(objectMetaKey(bucket, key))
	})
}

// ListAllObjectMetas returns all object metadata for a given bucket.
func (m *MetadataStore) ListAllObjectMetas(bucket string) ([]ObjectInfo, error) {
	var objects []ObjectInfo
	prefix := []byte(bucket + "\x00")
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, v = c.Next() {
			var info ObjectInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			objects = append(objects, info)
		}
		return nil
	})
	return objects, err
}

// CountObjectMetas counts the number of objects in a bucket without deserializing metadata.
// This is much more efficient than ListAllObjectMetas when only the count is needed.
func (m *MetadataStore) CountObjectMetas(bucket string) (int, error) {
	count := 0
	prefix := []byte(bucket + "\x00")
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, _ = c.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// DeleteAllObjectMetas removes all object metadata for a given bucket.
// Keys are collected first to avoid modifying bbolt during cursor iteration.
func (m *MetadataStore) DeleteAllObjectMetas(bucket string) error {
	prefix := []byte(bucket + "\x00")
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()

		// Collect keys first to avoid deleting during cursor iteration
		var keysToDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && len(k) >= len(prefix) && string(k[:len(prefix)]) == string(prefix); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}

		// Delete collected keys
		for _, key := range keysToDelete {
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// multipartKey returns the composite key for a multipart upload.
func multipartKey(bucket, key, uploadID string) []byte {
	return []byte(bucket + "\x00" + key + "\x00" + uploadID)
}

// MultipartMeta stores multipart upload metadata in bbolt.
type MultipartMeta struct {
	UploadID    string     `json:"uploadId"`
	Bucket      string     `json:"bucket"`
	Key         string     `json:"key"`
	ContentType string     `json:"contentType"`
	Created     time.Time  `json:"created"`
	Parts       []PartInfo `json:"parts"`
}

// PutMultipartMeta stores multipart upload metadata.
func (m *MetadataStore) PutMultipartMeta(meta *MultipartMeta) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(multipartBucket)
		data, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put(multipartKey(meta.Bucket, meta.Key, meta.UploadID), data)
	})
}

// GetMultipartMeta retrieves multipart upload metadata.
func (m *MetadataStore) GetMultipartMeta(bucket, key, uploadID string) (*MultipartMeta, error) {
	var meta MultipartMeta
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(multipartBucket)
		data := b.Get(multipartKey(bucket, key, uploadID))
		if data == nil {
			return fmt.Errorf("multipart upload not found: %s/%s/%s", bucket, key, uploadID)
		}
		return json.Unmarshal(data, &meta)
	})
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// DeleteMultipartMeta removes multipart upload metadata.
func (m *MetadataStore) DeleteMultipartMeta(bucket, key, uploadID string) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(multipartBucket)
		return b.Delete(multipartKey(bucket, key, uploadID))
	})
}
