package backup

import (
	"context"
	"io"
	"os"

	"emperror.dev/errors"
	"github.com/kurin/blazer/b2"
	"github.com/mholt/archiver/v3"
	"github.com/pterodactyl/wings/config"
	"github.com/pterodactyl/wings/remote"
	"github.com/pterodactyl/wings/server/filesystem"
)

type LocalBackup struct {
	Backup
}

var _ BackupInterface = (*LocalBackup)(nil)

func NewLocal(client remote.Client, uuid string, ignore string) *LocalBackup {
	return &LocalBackup{
		Backup{
			client:  client,
			Uuid:    uuid,
			Ignore:  ignore,
			adapter: LocalBackupAdapter,
		},
	}
}

// LocateLocal finds the backup for a server and returns the local path. This
// will obviously only work if the backup was created as a local backup.
func LocateLocal(client remote.Client, uuid string) (*LocalBackup, *b2.Attrs, error) {
	b := NewLocal(client, uuid, "")
	// Check if the backup exists on b2.
	id := config.Get().System.Backups.Backblazeid
	key := config.Get().System.Backups.Backblazekey
	ctx := context.Background()
	b2, err := b2.NewClient(ctx, id, key)
	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "backup: could not list buckets jj")
	}
	bucket := buckets[0]
	//get the folder name of basepath
	// var folderName string
	// for i := len(b.Path()) - 1; i >= 0; i-- {
	// 	if b.Path()[i] == '/' {
	// 		folderName = b.Path()[i+1:]
	// 		break
	// 	}
	// }
	destination := b.Backup.Uuid + ".tar.gz"
	b.log().Info("destination is " + destination)
	obj := bucket.Object(destination)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, nil, err
	}
	if attrs == nil {
		return nil, nil, os.ErrNotExist
	}

	return b, attrs, nil
}

// Remove removes a backup from the system.
func (b *LocalBackup) Remove() error {
	b.log().WithField("path", b.Path()).Info("removing backup from system")
	ctx := context.Background()
	id := config.Get().System.Backups.Backblazeid
	key := config.Get().System.Backups.Backblazekey
	b2, err := b2.NewClient(ctx, id, key)
	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		return errors.Wrap(err, "backup: could not list buckets jj")
	}
	bucket := buckets[0]
	//get the folder name of basepath
	// var folderName string
	// for i := len(b.Path()) - 1; i >= 0; i-- {
	// 	if b.Path()[i] == '/' {
	// 		folderName = b.Path()[i+1:]
	// 		break
	// 	}
	// }
	destination := b.Backup.Uuid + ".tar.gz"
	b.log().Info("destination is " + destination)
	obj := bucket.Object(destination)
	obj.Delete(ctx)

	return nil
}

// WithLogContext attaches additional context to the log output for this backup.
func (b *LocalBackup) WithLogContext(c map[string]interface{}) {
	b.logContext = c
}

// Generate generates a backup of the selected files and pushes it to the
// defined location for this instance.
func (b *LocalBackup) Generate(ctx context.Context, basePath, ignore string) (*ArchiveDetails, error) {
	defer os.Remove(b.Path())
	id := config.Get().System.Backups.Backblazeid
	key := config.Get().System.Backups.Backblazekey
	b2, err := b2.NewClient(ctx, id, key)
	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "backup: could not list buckets jj")
	}
	bucket := buckets[0]
	a := &filesystem.Archive{
		BasePath: basePath,
		Ignore:   ignore,
	}

	b.log().WithField("path", b.Path()).Info("creating backup for server")
	if err := a.Create(b.Path()); err != nil {
		return nil, err
	}
	b.log().Info("created backup successfully")
	// Upload the file to the bucket.

	//get the folder name of basepath
	// var folderName string
	// for i := len(basePath) - 1; i >= 0; i-- {
	// 	if basePath[i] == '/' {
	// 		folderName = basePath[i+1:]
	// 		break
	// 	}
	// }

	destination := b.Backup.Uuid + ".tar.gz"
	b.log().Info("destination is " + destination)
	if err := b.copyFile(ctx, bucket, b.Path(), destination); err != nil {
		return nil, errors.Wrap(err, "backup: could not upload archive to s3 JJ")
	}
	b.log().Info("uploaded backup successfully")
	ad, err := b.Details(ctx, nil)
	if err != nil {
		return nil, errors.WrapIf(err, "backup: failed to get archive details for local backup")
	}
	return ad, nil
}

func (b *LocalBackup) copyFile(ctx context.Context, bucket *b2.Bucket, src, dst string) error {
	b.log().Info("starting JJ backup")

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	obj := bucket.Object(dst)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// Restore will walk over the archive and call the callback function for each
// file encountered.
func (b *LocalBackup) Restore(ctx context.Context, _ io.Reader, callback RestoreCallback) error {
	return archiver.Walk(b.Path(), func(f archiver.File) error {
		select {
		case <-ctx.Done():
			// Stop walking if the context is canceled.
			return archiver.ErrStopWalk
		default:
			if f.IsDir() {
				return nil
			}
			return callback(filesystem.ExtractNameFromArchive(f), f, f.Mode(), f.ModTime(), f.ModTime())
		}
	})
}
