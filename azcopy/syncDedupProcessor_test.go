// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package azcopy

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

// helper to create a test CopyTransferProcessor in dry-run mode that captures transfers
func newTestProcessor(fromTo common.FromTo) (*CopyTransferProcessor, *[]common.CopyTransfer) {
	var captured []common.CopyTransfer

	template := &common.CopyJobPartOrderRequest{
		JobID:  common.NewJobID(),
		FromTo: fromTo,
		Fpo:    common.EFolderPropertiesOption.NoFolders(),
		SourceRoot: common.ResourceString{
			Value: "https://account.blob.core.windows.net/container",
		},
		DestinationRoot: common.ResourceString{
			Value: "https://account.blob.core.windows.net/container",
		},
	}

	dryrunHandler := func(req common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		captured = append(captured, req.Transfers.List...)
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}

	proc := NewCopyTransferProcessor(
		false,
		template,
		10000,
		template.SourceRoot,
		template.DestinationRoot,
		func(bool) {},
		func() {},
		false,
		true, // dryrun mode
		dryrunHandler,
	)

	return proc, &captured
}

func TestDedupProcessor_RoutesToDedupWhenHashMatches(t *testing.T) {
	a := assert.New(t)

	// Set up hash indexer with a destination object
	hashIndexer := traverser.NewHashIndexer()
	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	existingDestObj := traverser.StoredObject{
		Name:         "existing.txt",
		RelativePath: "old/existing.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}
	hashIndexer.Store(existingDestObj)

	// Create processors
	dedupProc, dedupCaptures := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, normalCaptures := newTestProcessor(common.EFromTo.LocalBlob())

	// Create the dedup processor
	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	// Process a source object that has the same hash but different path
	sourceObj := traverser.StoredObject{
		Name:         "newname.txt",
		RelativePath: "new/newname.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	// Should be routed to dedup (not normal)
	a.Equal(int64(1), dp.DedupCount())
	a.Equal(int64(0), dp.NormalCount())

	// Force dispatch to see captured transfers
	_, _ = dedupProc.DispatchFinalPart()
	_, _ = normalProc.DispatchFinalPart()

	a.Equal(1, len(*dedupCaptures))
	a.Equal(0, len(*normalCaptures))
}

func TestDedupProcessor_RoutesToNormalWhenNoHashMatch(t *testing.T) {
	a := assert.New(t)

	// Set up hash indexer with a destination object
	hashIndexer := traverser.NewHashIndexer()
	destHash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	sourceHash := []byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0}

	existingDestObj := traverser.StoredObject{
		Name:         "existing.txt",
		RelativePath: "old/existing.txt",
		Size:         1024,
		Md5:          destHash,
		EntityType:   common.EEntityType.File(),
	}
	hashIndexer.Store(existingDestObj)

	// Create processors
	dedupProc, dedupCaptures := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, normalCaptures := newTestProcessor(common.EFromTo.LocalBlob())

	// Create the dedup processor
	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	// Process a source object with a DIFFERENT hash
	sourceObj := traverser.StoredObject{
		Name:         "newfile.txt",
		RelativePath: "dir/newfile.txt",
		Size:         2048,
		Md5:          sourceHash,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	// Should be routed to normal (not dedup)
	a.Equal(int64(0), dp.DedupCount())
	a.Equal(int64(1), dp.NormalCount())

	// Force dispatch
	_, _ = dedupProc.DispatchFinalPart()
	_, _ = normalProc.DispatchFinalPart()

	a.Equal(0, len(*dedupCaptures))
	a.Equal(1, len(*normalCaptures))
}

func TestDedupProcessor_RoutesToNormalWhenSourceHasNoHash(t *testing.T) {
	a := assert.New(t)

	hashIndexer := traverser.NewHashIndexer()
	destHash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	hashIndexer.Store(traverser.StoredObject{
		RelativePath: "existing.txt",
		Size:         1024,
		Md5:          destHash,
		EntityType:   common.EEntityType.File(),
	})

	dedupProc, _ := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	// Process a source object with NO hash
	sourceObj := traverser.StoredObject{
		Name:         "nomd5.txt",
		RelativePath: "dir/nomd5.txt",
		Size:         1024,
		Md5:          nil,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	a.Equal(int64(0), dp.DedupCount())
	a.Equal(int64(1), dp.NormalCount())
}

func TestDedupProcessor_SkipsSamePathMatch(t *testing.T) {
	a := assert.New(t)

	// If the hash matches but the path is the same, it's the same file — skip dedup
	hashIndexer := traverser.NewHashIndexer()
	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	hashIndexer.Store(traverser.StoredObject{
		RelativePath: "same/path.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	})

	dedupProc, _ := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	// Process source with same path and hash
	sourceObj := traverser.StoredObject{
		Name:         "path.txt",
		RelativePath: "same/path.txt",
		Size:         1024,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	// Should NOT use dedup (same path = same file), should go to normal
	a.Equal(int64(0), dp.DedupCount())
	a.Equal(int64(1), dp.NormalCount())
}

func TestDedupProcessor_SkipsSizeMismatch(t *testing.T) {
	a := assert.New(t)

	// Hash match but different size should not trigger dedup (extra safety)
	hashIndexer := traverser.NewHashIndexer()
	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	hashIndexer.Store(traverser.StoredObject{
		RelativePath: "old/existing.txt",
		Size:         1024, // different size
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	})

	dedupProc, _ := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	sourceObj := traverser.StoredObject{
		Name:         "newname.txt",
		RelativePath: "new/newname.txt",
		Size:         2048, // different size from dest
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	// Should NOT use dedup due to size mismatch
	a.Equal(int64(0), dp.DedupCount())
	a.Equal(int64(1), dp.NormalCount())
}

func TestDedupProcessor_FoldersAlwaysGoNormal(t *testing.T) {
	a := assert.New(t)

	hashIndexer := traverser.NewHashIndexer()
	dedupProc, _ := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	// Folders should never go through dedup
	folderObj := traverser.StoredObject{
		Name:         "mydir",
		RelativePath: "mydir",
		EntityType:   common.EEntityType.Folder(),
	}

	err := dp.ProcessTransfer(folderObj)
	a.NoError(err)

	a.Equal(int64(0), dp.DedupCount())
	a.Equal(int64(1), dp.NormalCount())
}

func TestDedupProcessor_MixedMD5OnDestination(t *testing.T) {
	a := assert.New(t)

	// Simulate destination where some blobs have MD5 and some don't.
	// Only blobs WITH MD5 can serve as dedup sources.
	hashIndexer := traverser.NewHashIndexer()

	hashA := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	// Blob with MD5 (will be indexed)
	hashIndexer.Store(traverser.StoredObject{
		RelativePath: "has-md5/A.txt",
		Size:         100,
		Md5:          hashA,
		EntityType:   common.EEntityType.File(),
	})

	// Blob without MD5 (will NOT be indexed — just increments missing count)
	hashIndexer.Store(traverser.StoredObject{
		RelativePath: "no-md5/B.txt",
		Size:         200,
		Md5:          nil,
		EntityType:   common.EEntityType.File(),
	})

	a.Equal(1, hashIndexer.IndexedCount())
	a.Equal(int64(1), hashIndexer.MissingHashCount())

	dedupProc, dedupCaptures := newTestProcessor(common.EFromTo.BlobBlob())
	normalProc, normalCaptures := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), false, "")

	// Source file 1: same hash as A.txt → should dedup
	err := dp.ProcessTransfer(traverser.StoredObject{
		Name:         "renamed.txt",
		RelativePath: "new/renamed.txt",
		Size:         100,
		Md5:          hashA,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	// Source file 2: has a hash that matches B.txt's content, but B.txt has no MD5
	// in the indexer so there's no match → should go normal
	hashB := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00}
	err = dp.ProcessTransfer(traverser.StoredObject{
		Name:         "cant-dedup.txt",
		RelativePath: "other/cant-dedup.txt",
		Size:         200,
		Md5:          hashB,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	// Source file 3: completely new content → should go normal
	hashNew := []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00}
	err = dp.ProcessTransfer(traverser.StoredObject{
		Name:         "brand-new.txt",
		RelativePath: "fresh/brand-new.txt",
		Size:         300,
		Md5:          hashNew,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	// Verify routing
	a.Equal(int64(1), dp.DedupCount(), "Only 1 file should dedup (matches blob with MD5)")
	a.Equal(int64(2), dp.NormalCount(), "2 files should go normal (no matching MD5 in index)")

	// Verify actual transfers
	_, _ = dedupProc.DispatchFinalPart()
	_, _ = normalProc.DispatchFinalPart()

	a.Equal(1, len(*dedupCaptures), "1 dedup transfer captured")
	a.Equal(2, len(*normalCaptures), "2 normal transfers captured")
}

// helper to create a test CopyTransferProcessor with service-level roots for cross-container tests
func newCrossContainerTestProcessor(fromTo common.FromTo) (*CopyTransferProcessor, *[]common.CopyTransfer) {
	var captured []common.CopyTransfer

	template := &common.CopyJobPartOrderRequest{
		JobID:  common.NewJobID(),
		FromTo: fromTo,
		Fpo:    common.EFolderPropertiesOption.NoFolders(),
		SourceRoot: common.ResourceString{
			Value: "https://account.blob.core.windows.net", // service-level
		},
		DestinationRoot: common.ResourceString{
			Value: "https://account.blob.core.windows.net", // service-level
		},
	}

	dryrunHandler := func(req common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
		captured = append(captured, req.Transfers.List...)
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}

	proc := NewCopyTransferProcessor(
		false,
		template,
		10000,
		template.SourceRoot,
		template.DestinationRoot,
		func(bool) {},
		func() {},
		false,
		true, // dryrun mode
		dryrunHandler,
	)

	return proc, &captured
}

func TestDedupProcessor_CrossContainer_RoutesToDedupFromDifferentContainer(t *testing.T) {
	a := assert.New(t)

	// Set up hash indexer with an object from a DIFFERENT container ("archive")
	hashIndexer := traverser.NewHashIndexer()
	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	existingObj := traverser.StoredObject{
		Name:          "archived-file.txt",
		RelativePath:  "data/archived-file.txt",
		Size:          2048,
		Md5:           md5Hash,
		EntityType:    common.EEntityType.File(),
		ContainerName: "archive", // different from destination container "destcontainer"
	}
	hashIndexer.Store(existingObj)

	// Create processors with service-level roots (cross-container mode)
	dedupProc, dedupCaptures := newCrossContainerTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	// Create the dedup processor in cross-container mode
	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), true, "destcontainer")

	// Process a source object that has the same hash
	sourceObj := traverser.StoredObject{
		Name:         "newfile.txt",
		RelativePath: "new/newfile.txt",
		Size:         2048,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	// Should be routed to dedup
	a.Equal(int64(1), dp.DedupCount())
	a.Equal(int64(0), dp.NormalCount())

	// Force dispatch and verify the transfer paths include container names
	_, _ = dedupProc.DispatchFinalPart()

	a.Equal(1, len(*dedupCaptures))
	transfer := (*dedupCaptures)[0]

	// Source path should include the source container name ("archive")
	a.Contains(transfer.Source, "/archive/")
	a.Contains(transfer.Source, "data/archived-file.txt")

	// Destination path should include the destination container name ("destcontainer")
	a.Contains(transfer.Destination, "/destcontainer/")
	a.Contains(transfer.Destination, "new/newfile.txt")
}

func TestDedupProcessor_CrossContainer_SameContainerStillWorks(t *testing.T) {
	a := assert.New(t)

	// Set up hash indexer with an object from the SAME container ("destcontainer")
	hashIndexer := traverser.NewHashIndexer()
	md5Hash := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00}

	existingObj := traverser.StoredObject{
		Name:          "original.txt",
		RelativePath:  "old/original.txt",
		Size:          512,
		Md5:           md5Hash,
		EntityType:    common.EEntityType.File(),
		ContainerName: "destcontainer", // same as destination container
	}
	hashIndexer.Store(existingObj)

	// Create processors with service-level roots (cross-container mode)
	dedupProc, dedupCaptures := newCrossContainerTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	// Create the dedup processor in cross-container mode
	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), true, "destcontainer")

	// Process a source object that has the same hash
	sourceObj := traverser.StoredObject{
		Name:         "copy.txt",
		RelativePath: "copies/copy.txt",
		Size:         512,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	}

	err := dp.ProcessTransfer(sourceObj)
	a.NoError(err)

	a.Equal(int64(1), dp.DedupCount())

	_, _ = dedupProc.DispatchFinalPart()
	a.Equal(1, len(*dedupCaptures))

	transfer := (*dedupCaptures)[0]

	// Source should reference the same container
	a.Contains(transfer.Source, "/destcontainer/")
	a.Contains(transfer.Source, "old/original.txt")

	// Destination should also reference the destination container
	a.Contains(transfer.Destination, "/destcontainer/")
	a.Contains(transfer.Destination, "copies/copy.txt")
}

func TestDedupProcessor_CrossContainer_NoMatchGoesNormal(t *testing.T) {
	a := assert.New(t)

	// Set up hash indexer with an object in "archive" container
	hashIndexer := traverser.NewHashIndexer()
	archiveHash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	hashIndexer.Store(traverser.StoredObject{
		Name:          "archived.txt",
		RelativePath:  "archived.txt",
		Size:          1024,
		Md5:           archiveHash,
		EntityType:    common.EEntityType.File(),
		ContainerName: "archive",
	})

	dedupProc, dedupCaptures := newCrossContainerTestProcessor(common.EFromTo.BlobBlob())
	normalProc, normalCaptures := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), true, "destcontainer")

	// Process a source object with a DIFFERENT hash
	differentHash := []byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0}
	err := dp.ProcessTransfer(traverser.StoredObject{
		Name:         "unique.txt",
		RelativePath: "unique.txt",
		Size:         2048,
		Md5:          differentHash,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	a.Equal(int64(0), dp.DedupCount())
	a.Equal(int64(1), dp.NormalCount())

	_, _ = dedupProc.DispatchFinalPart()
	_, _ = normalProc.DispatchFinalPart()

	a.Equal(0, len(*dedupCaptures))
	a.Equal(1, len(*normalCaptures))
}

func TestDedupProcessor_CrossContainer_MultipleContainerSources(t *testing.T) {
	a := assert.New(t)

	// Set up hash indexer with objects from TWO different containers
	hashIndexer := traverser.NewHashIndexer()
	hashFromArchive := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	hashFromBackups := []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00}

	hashIndexer.Store(traverser.StoredObject{
		Name:          "archived.txt",
		RelativePath:  "data/archived.txt",
		Size:          1024,
		Md5:           hashFromArchive,
		EntityType:    common.EEntityType.File(),
		ContainerName: "archive",
	})
	hashIndexer.Store(traverser.StoredObject{
		Name:          "backup.txt",
		RelativePath:  "bak/backup.txt",
		Size:          2048,
		Md5:           hashFromBackups,
		EntityType:    common.EEntityType.File(),
		ContainerName: "backups",
	})

	// Create processors with service-level roots
	dedupProc, dedupCaptures := newCrossContainerTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), true, "destcontainer")

	// Process a file matching archive content
	err := dp.ProcessTransfer(traverser.StoredObject{
		Name:         "file1.txt",
		RelativePath: "new/file1.txt",
		Size:         1024,
		Md5:          hashFromArchive,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	// Process a file matching backups content
	err = dp.ProcessTransfer(traverser.StoredObject{
		Name:         "file2.txt",
		RelativePath: "new/file2.txt",
		Size:         2048,
		Md5:          hashFromBackups,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	a.Equal(int64(2), dp.DedupCount())
	a.Equal(int64(0), dp.NormalCount())

	_, _ = dedupProc.DispatchFinalPart()
	a.Equal(2, len(*dedupCaptures))

	// First transfer should reference "archive" container
	a.Contains((*dedupCaptures)[0].Source, "/archive/")
	a.Contains((*dedupCaptures)[0].Source, "data/archived.txt")
	a.Contains((*dedupCaptures)[0].Destination, "/destcontainer/")
	a.Contains((*dedupCaptures)[0].Destination, "new/file1.txt")

	// Second transfer should reference "backups" container
	a.Contains((*dedupCaptures)[1].Source, "/backups/")
	a.Contains((*dedupCaptures)[1].Source, "bak/backup.txt")
	a.Contains((*dedupCaptures)[1].Destination, "/destcontainer/")
	a.Contains((*dedupCaptures)[1].Destination, "new/file2.txt")
}

func TestDedupProcessor_CrossContainer_EmptyContainerNameIgnored(t *testing.T) {
	a := assert.New(t)

	// Edge case: what if a stored object has empty ContainerName?
	hashIndexer := traverser.NewHashIndexer()
	md5Hash := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

	// Store with empty container name (shouldn't happen in practice, but handle gracefully)
	hashIndexer.Store(traverser.StoredObject{
		Name:          "orphan.txt",
		RelativePath:  "orphan.txt",
		Size:          100,
		Md5:           md5Hash,
		EntityType:    common.EEntityType.File(),
		ContainerName: "", // empty!
	})

	dedupProc, dedupCaptures := newCrossContainerTestProcessor(common.EFromTo.BlobBlob())
	normalProc, _ := newTestProcessor(common.EFromTo.LocalBlob())

	dp := newSyncDedupProcessor(hashIndexer, dedupProc, normalProc, common.EFromTo.BlobBlob(), true, "destcontainer")

	err := dp.ProcessTransfer(traverser.StoredObject{
		Name:         "match.txt",
		RelativePath: "match.txt",
		Size:         100,
		Md5:          md5Hash,
		EntityType:   common.EEntityType.File(),
	})
	a.NoError(err)

	// Should still be processed as dedup (with empty container the source path is just the relative path)
	a.Equal(int64(1), dp.DedupCount())
	_, _ = dedupProc.DispatchFinalPart()
	a.Equal(1, len(*dedupCaptures))

	// Source should NOT have a double slash (empty container produces just /relativePath)
	a.Equal("/orphan.txt", (*dedupCaptures)[0].Source)
	a.Contains((*dedupCaptures)[0].Destination, "/destcontainer/")
}
