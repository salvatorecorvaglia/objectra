package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketsBucket   = []byte("buckets")
	objectsBucket   = []byte("objects")
	multipartBucket = []byte("multipart")
)

// MetadataStore manages bucket and object metadata using per-bucket bbolt databases.
type MetadataStore struct {
	globalDB      *bolt.DB
	activeBuckets map[string]*bolt.DB
	mu            sync.RWMutex
	dataDir       string
}

// NewMetadataStore opens or creates the central database and sets up the metadata directory.
func NewMetadataStore(dataDir string) (*MetadataStore, error) {
	metaDir := filepath.Join(dataDir, "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create metadata directory: %w", err)
	}

	globalPath := filepath.Join(dataDir, "objectra.db")
	globalDB, err := bolt.Open(globalPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open central metadata db: %w", err)
	}

	err = globalDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketsBucket)
		return err
	})
	if err != nil {
		globalDB.Close()
		return nil, fmt.Errorf("failed to initialize central metadata db: %w", err)
	}

	m := &MetadataStore{
		globalDB:      globalDB,
		activeBuckets: make(map[string]*bolt.DB),
		dataDir:       dataDir,
	}

	if err := m.migrateIfNecessary(); err != nil {
		m.Close()
		return nil, fmt.Errorf("failed to run startup metadata migration: %w", err)
	}

	return m, nil
}

// Close closes all databases.
func (m *MetadataStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for bucket, db := range m.activeBuckets {
		if err := db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close db for bucket %s: %w", bucket, err))
		}
	}
	m.activeBuckets = make(map[string]*bolt.DB)

	if err := m.globalDB.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close central metadata db: %w", err))
	}

	return errors.Join(errs...)
}

func (m *MetadataStore) getBucketDB(bucket string) (*bolt.DB, error) {
	m.mu.RLock()
	if db, ok := m.activeBuckets[bucket]; ok {
		m.mu.RUnlock()
		return db, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check under write lock
	if db, ok := m.activeBuckets[bucket]; ok {
		return db, nil
	}

	dbPath := filepath.Join(m.dataDir, "metadata", bucket+".db")
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata db for bucket %s: %w", bucket, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(objectsBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(multipartBucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize metadata db for bucket %s: %w", bucket, err)
	}

	m.activeBuckets[bucket] = db
	return db, nil
}

func (m *MetadataStore) CloseAndRemoveBucketDB(bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	db, ok := m.activeBuckets[bucket]
	if ok {
		_ = db.Close()
		delete(m.activeBuckets, bucket)
	}

	dbPath := filepath.Join(m.dataDir, "metadata", bucket+".db")
	return os.Remove(dbPath)
}

func (m *MetadataStore) migrateIfNecessary() error {
	var objectsBucketExists, multipartBucketExists bool
	err := m.globalDB.View(func(tx *bolt.Tx) error {
		objectsBucketExists = tx.Bucket(objectsBucket) != nil
		multipartBucketExists = tx.Bucket(multipartBucket) != nil
		return nil
	})
	if err != nil {
		return err
	}

	if !objectsBucketExists && !multipartBucketExists {
		return nil
	}

	log.Println("[Migration] Migrating old single-database metadata to per-bucket databases...")

	if objectsBucketExists {
		var errs []error
		err = m.globalDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(objectsBucket)
			return b.ForEach(func(k, v []byte) error {
				parts := bytes.SplitN(k, []byte("\x00"), 2)
				if len(parts) != 2 {
					return nil
				}
				bucketName := string(parts[0])

				bucketDB, err := m.getBucketDB(bucketName)
				if err != nil {
					errs = append(errs, err)
					return nil
				}

				err = bucketDB.Update(func(btx *bolt.Tx) error {
					bb, err := btx.CreateBucketIfNotExists(objectsBucket)
					if err != nil {
						return err
					}
					return bb.Put(k, v)
				})
				if err != nil {
					errs = append(errs, err)
				}
				return nil
			})
		})
		if err != nil {
			return err
		}
		if len(errs) > 0 {
			return errs[0]
		}
	}

	if multipartBucketExists {
		var errs []error
		err = m.globalDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(multipartBucket)
			return b.ForEach(func(k, v []byte) error {
				parts := bytes.SplitN(k, []byte("\x00"), 3)
				if len(parts) < 1 {
					return nil
				}
				bucketName := string(parts[0])

				bucketDB, err := m.getBucketDB(bucketName)
				if err != nil {
					errs = append(errs, err)
					return nil
				}

				err = bucketDB.Update(func(btx *bolt.Tx) error {
					bb, err := btx.CreateBucketIfNotExists(multipartBucket)
					if err != nil {
						return err
					}
					return bb.Put(k, v)
				})
				if err != nil {
					errs = append(errs, err)
				}
				return nil
			})
		})
		if err != nil {
			return err
		}
		if len(errs) > 0 {
			return errs[0]
		}
	}

	err = m.globalDB.Update(func(tx *bolt.Tx) error {
		if objectsBucketExists {
			_ = tx.DeleteBucket(objectsBucket)
		}
		if multipartBucketExists {
			_ = tx.DeleteBucket(multipartBucket)
		}
		return nil
	})
	if err != nil {
		return err
	}

	log.Println("[Migration] Migration completed successfully.")
	return nil
}

// PutBucket stores bucket metadata.
func (m *MetadataStore) PutBucket(info *BucketInfo) error {
	return m.globalDB.Update(func(tx *bolt.Tx) error {
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
	err := m.globalDB.View(func(tx *bolt.Tx) error {
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

// DeleteBucket removes bucket metadata and deletes its DB file.
func (m *MetadataStore) DeleteBucket(name string) error {
	err := m.globalDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		return b.Delete([]byte(name))
	})
	if err != nil {
		return err
	}
	return m.CloseAndRemoveBucketDB(name)
}

// ListBuckets returns all stored bucket metadata.
func (m *MetadataStore) ListBuckets() ([]BucketInfo, error) {
	var buckets []BucketInfo
	err := m.globalDB.View(func(tx *bolt.Tx) error {
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

// BucketExists checks if a bucket exists in the central metadata store.
func (m *MetadataStore) BucketExists(name string) (bool, error) {
	exists := false
	err := m.globalDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		exists = b.Get([]byte(name)) != nil
		return nil
	})
	return exists, err
}

// PutBucketCORS sets CORS configuration for a bucket.
func (m *MetadataStore) PutBucketCORS(bucket string, cors *CORSConfiguration) error {
	return m.globalDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		info.CORS = cors
		newData, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(bucket), newData)
	})
}

// GetBucketCORS gets CORS configuration for a bucket.
func (m *MetadataStore) GetBucketCORS(bucket string) (*CORSConfiguration, error) {
	var cors *CORSConfiguration
	err := m.globalDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		cors = info.CORS
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cors, nil
}

// DeleteBucketCORS deletes CORS configuration for a bucket.
func (m *MetadataStore) DeleteBucketCORS(bucket string) error {
	return m.globalDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		info.CORS = nil
		newData, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(bucket), newData)
	})
}

// PutBucketVersioning sets versioning configuration for a bucket.
func (m *MetadataStore) PutBucketVersioning(bucket string, status string) error {
	return m.globalDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		info.Versioning = status
		newData, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(bucket), newData)
	})
}

// GetBucketVersioning gets versioning configuration for a bucket.
func (m *MetadataStore) GetBucketVersioning(bucket string) (string, error) {
	var status string
	err := m.globalDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		status = info.Versioning
		return nil
	})
	return status, err
}

// SetBucketPublic sets the public status of a bucket.
func (m *MetadataStore) SetBucketPublic(bucket string, public bool) error {
	return m.globalDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		info.IsPublic = public
		newData, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(bucket), newData)
	})
}

// IsBucketPublic gets the public status of a bucket.
func (m *MetadataStore) IsBucketPublic(bucket string) (bool, error) {
	var public bool
	err := m.globalDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketsBucket)
		data := b.Get([]byte(bucket))
		if data == nil {
			return fmt.Errorf("bucket not found: %s", bucket)
		}
		var info BucketInfo
		if err := json.Unmarshal(data, &info); err != nil {
			return err
		}
		public = info.IsPublic
		return nil
	})
	return public, err
}

func objectMetaKey(bucket, key string) []byte {
	return []byte(bucket + "\x00" + key)
}

func objectMetaKeyVersion(bucket, key, versionID string) []byte {
	return []byte(bucket + "\x00" + key + "\x00" + versionID)
}

// PutObjectMeta stores object metadata (both the latest pointer and the historical record).
func (m *MetadataStore) PutObjectMeta(info *ObjectInfo) error {
	db, err := m.getBucketDB(info.Bucket)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)

		info.IsLatest = true

		// Update old latest version if present
		oldData := b.Get(objectMetaKey(info.Bucket, info.Key))
		if oldData != nil {
			var oldInfo ObjectInfo
			if err := json.Unmarshal(oldData, &oldInfo); err == nil {
				if oldInfo.VersionID != "" && oldInfo.VersionID != info.VersionID {
					oldInfo.IsLatest = false
					oldHistData, _ := json.Marshal(oldInfo)
					_ = b.Put(objectMetaKeyVersion(oldInfo.Bucket, oldInfo.Key, oldInfo.VersionID), oldHistData)
				}
			}
		}

		data, err := json.Marshal(info)
		if err != nil {
			return err
		}
		// Write historical version if version ID is present
		if info.VersionID != "" {
			err = b.Put(objectMetaKeyVersion(info.Bucket, info.Key, info.VersionID), data)
			if err != nil {
				return err
			}
		}
		// Write/Update latest version pointer
		return b.Put(objectMetaKey(info.Bucket, info.Key), data)
	})
}

// GetObjectMeta retrieves object metadata. If versionID is empty, retrieves the latest version.
func (m *MetadataStore) GetObjectMeta(bucket, key, versionID string) (*ObjectInfo, error) {
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return nil, err
	}
	var info ObjectInfo
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		var data []byte
		if versionID != "" {
			data = b.Get(objectMetaKeyVersion(bucket, key, versionID))
		} else {
			data = b.Get(objectMetaKey(bucket, key))
		}
		if data == nil {
			return fmt.Errorf("object not found: %s/%s (version=%s)", bucket, key, versionID)
		}
		return json.Unmarshal(data, &info)
	})
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// DeleteObjectMeta removes object metadata. If versionID is specified, deletes only that version.
// If latest is deleted, updates the latest pointer to the next newest version in history.
func (m *MetadataStore) DeleteObjectMeta(bucket, key, versionID string) error {
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		if versionID != "" {
			// Delete specific version
			err = b.Delete(objectMetaKeyVersion(bucket, key, versionID))
			if err != nil {
				return err
			}

			// If this was the latest pointer, we must find the next newest version
			latestData := b.Get(objectMetaKey(bucket, key))
			if latestData != nil {
				var latestInfo ObjectInfo
				if err := json.Unmarshal(latestData, &latestInfo); err == nil && latestInfo.VersionID == versionID {
					// We just deleted the latest version! Let's find the next newest version from history
					nextLatest, err := m.findNextNewestVersion(tx, bucket, key, versionID)
					if err == nil && nextLatest != nil {
						nextLatest.IsLatest = true
						nextData, _ := json.Marshal(nextLatest)
						_ = b.Put(objectMetaKey(bucket, key), nextData)
						if nextLatest.VersionID != "" {
							_ = b.Put(objectMetaKeyVersion(bucket, key, nextLatest.VersionID), nextData)
						}
					} else {
						_ = b.Delete(objectMetaKey(bucket, key))
					}
				}
			}
			return nil
		}

		// Delete all metadata (pointer and all historical versions)
		_ = b.Delete(objectMetaKey(bucket, key))
		prefix := []byte(bucket + "\x00" + key + "\x00")
		c := b.Cursor()
		var keysToDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}
		for _, k := range keysToDelete {
			_ = b.Delete(k)
		}
		return nil
	})
}

func (m *MetadataStore) findNextNewestVersion(tx *bolt.Tx, bucket, key, deletedVersionID string) (*ObjectInfo, error) {
	b := tx.Bucket(objectsBucket)
	prefix := []byte(bucket + "\x00" + key + "\x00")
	c := b.Cursor()

	var newest *ObjectInfo
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		var info ObjectInfo
		if err := json.Unmarshal(v, &info); err == nil {
			if info.VersionID != deletedVersionID {
				if newest == nil || info.LastModified.After(newest.LastModified) {
					newest = &info
				}
			}
		}
	}
	return newest, nil
}

// ListAllObjectMetas returns all object metadata for a given bucket (latest versions only).
func (m *MetadataStore) ListAllObjectMetas(bucket string) ([]ObjectInfo, error) {
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return nil, err
	}
	var objects []ObjectInfo
	prefix := []byte(bucket + "\x00")
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			// Skip version history records (which contain another \x00)
			objectKey := string(k[len(prefix):])
			if strings.Contains(objectKey, "\x00") {
				continue
			}

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

// CountObjectMetas counts latest objects in a bucket.
func (m *MetadataStore) CountObjectMetas(bucket string) (int, error) {
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return 0, err
	}
	count := 0
	prefix := []byte(bucket + "\x00")
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			objectKey := string(k[len(prefix):])
			if strings.Contains(objectKey, "\x00") {
				continue
			}
			count++
		}
		return nil
	})
	return count, err
}

// DeleteAllObjectMetas removes all object metadata for a given bucket.
func (m *MetadataStore) DeleteAllObjectMetas(bucket string) error {
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objectsBucket)
		c := b.Cursor()
		var keysToDelete [][]byte
		prefix := []byte(bucket + "\x00")
		for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			keysToDelete = append(keysToDelete, keyCopy)
		}
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

// MultipartMeta stores multipart upload metadata.
type MultipartMeta struct {
	UploadID             string     `json:"uploadId"`
	Bucket               string     `json:"bucket"`
	Key                  string     `json:"key"`
	ContentType          string     `json:"contentType"`
	Created              time.Time  `json:"created"`
	Parts                []PartInfo `json:"parts"`
	SSECustomerAlgorithm string     `json:"sseCustomerAlgorithm,omitempty"`
	SSECustomerKey       []byte     `json:"sseCustomerKey,omitempty"`
	SSECustomerKeyMD5    string     `json:"sseCustomerKeyMD5,omitempty"`
}

// PutMultipartMeta stores multipart upload metadata.
func (m *MetadataStore) PutMultipartMeta(meta *MultipartMeta) error {
	db, err := m.getBucketDB(meta.Bucket)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
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
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return nil, err
	}
	var meta MultipartMeta
	err = db.View(func(tx *bolt.Tx) error {
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
	db, err := m.getBucketDB(bucket)
	if err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(multipartBucket)
		return b.Delete(multipartKey(bucket, key, uploadID))
	})
}
