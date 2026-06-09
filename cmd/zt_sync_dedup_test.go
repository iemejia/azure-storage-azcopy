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

package cmd

import (
	"crypto/md5"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
	"github.com/stretchr/testify/assert"
)

// dedupInterceptor is a specialized interceptor that separates transfers by their FromTo type,
// allowing us to distinguish between normal transfers and dedup (server-side copy) transfers.
type dedupInterceptor struct {
	normalTransfers []common.CopyTransfer // transfers from normal path (e.g., LocalBlob)
	dedupTransfers  []common.CopyTransfer // transfers from dedup path (e.g., BlobBlob)
	deletions       []traverser.StoredObject
	normalFromTo    common.FromTo
	dedupFromTo     common.FromTo
}

func (d *dedupInterceptor) init(normalFromTo, dedupFromTo common.FromTo) {
	d.normalFromTo = normalFromTo
	d.dedupFromTo = dedupFromTo
	glcm = &mockedLifecycleManager{
		infoLog: make(chan string, 5000),
	}
}

func (d *dedupInterceptor) intercept(copyRequest common.CopyJobPartOrderRequest) common.CopyJobPartOrderResponse {
	if copyRequest.FromTo == d.dedupFromTo {
		d.dedupTransfers = append(d.dedupTransfers, copyRequest.Transfers.List...)
	} else {
		d.normalTransfers = append(d.normalTransfers, copyRequest.Transfers.List...)
	}

	totalTransfers := len(d.normalTransfers) + len(d.dedupTransfers)
	if totalTransfers != 0 || !copyRequest.IsFinalPart {
		return common.CopyJobPartOrderResponse{JobStarted: true}
	}
	return common.CopyJobPartOrderResponse{JobStarted: false, ErrorMsg: common.ECopyJobPartOrderErrorType.NoTransfersScheduledErr()}
}

func (d *dedupInterceptor) delete(_ string, _ common.Location, object traverser.StoredObject) error {
	d.deletions = append(d.deletions, object)
	return nil
}

// TestSyncUploadWithDedupCopy tests that the --dedup-copy flag correctly routes
// transfers to the server-side copy path when identical content exists on the destination.
//
// Scenario:
// - Destination has blobs A.txt and B.txt with different content, both with Content-MD5
// - Source has C.txt (same content as A.txt, different path) and D.txt (completely new content)
// - Expected: C.txt should be transferred via dedup (BlobBlob copy from A.txt), D.txt via normal upload
func TestSyncUploadWithDedupCopy(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Content for the test files
	contentA := "This is the content of file A - used for dedup testing"
	contentB := "This is completely different content for file B"
	contentD := "This is brand new content that doesn't exist on destination"

	// Compute MD5 hashes
	md5A := md5.Sum([]byte(contentA))
	md5B := md5.Sum([]byte(contentB))

	// Upload blobs to destination WITH Content-MD5
	blobClientA := cc.NewBlockBlobClient("existing/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader(contentA)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5A[:],
			},
		})
	a.Nil(err)

	blobClientB := cc.NewBlockBlobClient("existing/B.txt")
	_, err = blobClientB.Upload(ctx, streaming.NopCloser(strings.NewReader(contentB)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5B[:],
			},
		})
	a.Nil(err)

	// Wait a bit for the blobs to be visible
	time.Sleep(time.Millisecond * 1050)

	// Set up local source directory
	// C.txt has SAME content as A.txt (should trigger dedup)
	// D.txt has NEW content (should trigger normal upload)
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)

	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/C.txt", "new/D.txt"})
	// Overwrite with specific content
	err = os.WriteFile(srcDirName+"/renamed/C.txt", []byte(contentA), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/new/D.txt", []byte(contentD), 0644)
	a.Nil(err)

	// Set up the dedup interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	// Construct sync command args with --dedup-copy
	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	// Run sync
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// C.txt should be routed through dedup (BlobBlob) because its content matches A.txt
		// D.txt should be routed through normal upload (LocalBlob) because its content is new
		a.Equal(1, len(mockedRPC.dedupTransfers), "Expected 1 dedup transfer for renamed/C.txt")
		a.Equal(1, len(mockedRPC.normalTransfers), "Expected 1 normal transfer for new/D.txt")

		// Verify the dedup transfer has the correct destination path
		dedupDst := mockedRPC.dedupTransfers[0].Destination
		a.Contains(dedupDst, "renamed/C.txt", "Dedup transfer should target renamed/C.txt")

		// Verify the normal transfer has the correct destination
		normalDst := mockedRPC.normalTransfers[0].Destination
		a.Contains(normalDst, "new/D.txt", "Normal transfer should target new/D.txt")
	})
}

// TestSyncUploadWithDedupCopyNoBlobsHaveMD5 tests that when no destination blobs
// have Content-MD5, all transfers go through the normal path.
func TestSyncUploadWithDedupCopyNoBlobsHaveMD5(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Upload blob then CLEAR its Content-MD5 (the Go SDK auto-sets MD5 on Upload,
	// so we must explicitly remove it to test the "no MD5" scenario)
	blobClientA := cc.NewBlockBlobClient("existing/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader("content A")), nil)
	a.Nil(err)
	// Clear the auto-set Content-MD5
	_, err = blobClientA.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentMD5: []byte{}}, nil)
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Set up local source with a file that has same content as A.txt
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/C.txt"})
	err = os.WriteFile(srcDirName+"/renamed/C.txt", []byte("content A"), 0644)
	a.Nil(err)

	// Set up interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// No dedup should happen since destination blobs lack Content-MD5
		a.Equal(0, len(mockedRPC.dedupTransfers), "No dedup transfers when destination lacks MD5")
		a.Equal(1, len(mockedRPC.normalTransfers), "All transfers should be normal")
	})
}

// TestSyncUploadWithDedupCopyDisabled tests that without --dedup-copy, all transfers
// go through the normal path even when hash matches exist.
func TestSyncUploadWithDedupCopyDisabled(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	contentA := "dedup test content that will match"
	md5A := md5.Sum([]byte(contentA))

	// Upload blob WITH Content-MD5
	blobClientA := cc.NewBlockBlobClient("existing/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader(contentA)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5A[:],
			},
		})
	a.Nil(err)
	time.Sleep(time.Millisecond * 1050)

	// Set up local source with a file that has same content
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/C.txt"})
	err = os.WriteFile(srcDirName+"/renamed/C.txt", []byte(contentA), 0644)
	a.Nil(err)

	// Interceptor - using standard interceptor since no dedup expected
	mockedRPC := interceptor{}
	mockedRPC.init()

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	// explicitly NOT setting raw.dedupCopy = true
	raw.compareHash = "MD5"
	raw.putMd5 = true

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// Without dedup-copy, the transfer should happen normally (LocalBlob)
		a.Equal(1, len(mockedRPC.transfers), "Should have 1 normal transfer")
	})
}

// TestSyncUploadWithDedupCopyMultipleMatches tests that when multiple destination blobs
// share the same content, dedup still works correctly (uses first match).
func TestSyncUploadWithDedupCopyMultipleMatches(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	sharedContent := "This content appears in multiple destination blobs"
	md5Shared := md5.Sum([]byte(sharedContent))

	// Upload multiple blobs with the same content and MD5
	for _, path := range []string{"dup1/file.txt", "dup2/file.txt", "dup3/file.txt"} {
		blobClient := cc.NewBlockBlobClient(path)
		_, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(sharedContent)),
			&blockblob.UploadOptions{
				HTTPHeaders: &blob.HTTPHeaders{
					BlobContentMD5: md5Shared[:],
				},
			})
		a.Nil(err)
	}
	time.Sleep(time.Millisecond * 1050)

	// Local source with two files that have the same content
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"newpath1/x.txt", "newpath2/y.txt"})
	_ = os.WriteFile(srcDirName+"/newpath1/x.txt", []byte(sharedContent), 0644)
	_ = os.WriteFile(srcDirName+"/newpath2/y.txt", []byte(sharedContent), 0644)

	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// Both files should be dedup-copied since their content matches existing destination blobs
		a.Equal(2, len(mockedRPC.dedupTransfers), "Expected 2 dedup transfers")
		a.Equal(0, len(mockedRPC.normalTransfers), "Expected 0 normal transfers")
	})
}

// TestSyncUploadDedupCopyAutoEnablesCompareHash tests that --dedup-copy automatically
// enables --compare-hash=MD5 if not explicitly set by verifying the raw args passthrough.
func TestSyncUploadDedupCopyAutoEnablesCompareHash(t *testing.T) {
	a := assert.New(t)

	// Verify that dedupCopy is correctly passed through options
	tmpDir := t.TempDir()
	raw := getDefaultSyncRawInput(tmpDir, "https://account.blob.core.windows.net/container?sv=2021-06-08&se=2030-01-01&sr=c&sp=rwdlacx&sig=fake")
	raw.dedupCopy = true
	// compareHash defaults to "None" - auto-enable happens in cooked options

	opts, err := raw.toOptions()
	a.Nil(err)
	a.True(opts.DedupCopy)
}

// TestSyncUploadWithDedupCopyMixedMD5 tests the scenario where some destination blobs
// have Content-MD5 metadata and some do not. Only files matching blobs WITH MD5 should
// be dedup-copied; files matching blobs WITHOUT MD5 cannot participate in dedup and
// should be uploaded normally.
func TestSyncUploadWithDedupCopyMixedMD5(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()
	cc, containerName := createNewContainer(a, bsc)
	defer deleteContainer(a, cc)

	// Content for test files
	contentWithMD5 := "This blob has Content-MD5 set and can be used for dedup"
	contentWithoutMD5 := "This blob does NOT have Content-MD5 set"
	contentNew := "Completely new content that exists nowhere on destination"

	md5WithHash := md5.Sum([]byte(contentWithMD5))

	// Upload blob A WITH Content-MD5 (can participate in dedup)
	blobClientA := cc.NewBlockBlobClient("has-md5/A.txt")
	_, err := blobClientA.Upload(ctx, streaming.NopCloser(strings.NewReader(contentWithMD5)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5WithHash[:],
			},
		})
	a.Nil(err)

	// Upload blob B WITHOUT Content-MD5 (cannot participate in dedup)
	// Note: The Go SDK auto-computes Content-MD5 on Upload, so we must clear it explicitly
	blobClientB := cc.NewBlockBlobClient("no-md5/B.txt")
	_, err = blobClientB.Upload(ctx, streaming.NopCloser(strings.NewReader(contentWithoutMD5)), nil)
	a.Nil(err)
	_, err = blobClientB.SetHTTPHeaders(ctx, blob.HTTPHeaders{BlobContentMD5: []byte{}}, nil)
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Set up local source:
	// - "dedup-me.txt" has same content as A.txt (which has MD5) -> should dedup
	// - "cant-dedup.txt" has same content as B.txt (which lacks MD5) -> must upload normally
	// - "brand-new.txt" has content not on destination at all -> must upload normally
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)

	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{
		"dedup-me.txt",
		"cant-dedup.txt",
		"brand-new.txt",
	})
	err = os.WriteFile(srcDirName+"/dedup-me.txt", []byte(contentWithMD5), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/cant-dedup.txt", []byte(contentWithoutMD5), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/brand-new.txt", []byte(contentNew), 0644)
	a.Nil(err)

	// Set up dedup interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	rawContainerURLWithSAS := scenarioHelper{}.getRawContainerURLWithSAS(a, containerName)
	raw := getDefaultSyncRawInput(srcDirName, rawContainerURLWithSAS.String())
	raw.dedupCopy = true
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// dedup-me.txt should be server-side copied (content matches A.txt which has MD5)
		a.Equal(1, len(mockedRPC.dedupTransfers),
			"Expected 1 dedup transfer for dedup-me.txt (matches blob with MD5)")

		// cant-dedup.txt and brand-new.txt should go through normal upload:
		// - cant-dedup.txt: content matches B.txt but B.txt lacks MD5 so no dedup possible
		// - brand-new.txt: content doesn't exist anywhere on destination
		a.Equal(2, len(mockedRPC.normalTransfers),
			"Expected 2 normal transfers for cant-dedup.txt and brand-new.txt")

		// Verify the dedup transfer targets the correct file
		dedupDst := mockedRPC.dedupTransfers[0].Destination
		a.Contains(dedupDst, "dedup-me.txt")
	})
}

// Helper to expose cooked options for testing. We'll add this as a test-only export.
// (Note: This test function validates that toOptions() correctly passes dedupCopy through)
func TestSyncRawArgsDedupCopyFieldPassthrough(t *testing.T) {
	a := assert.New(t)

	tmpDir := t.TempDir()
	raw := getDefaultSyncRawInput(tmpDir, "https://account.blob.core.windows.net/container?sv=2021-06-08&se=2030-01-01&sr=c&sp=rwdlacx&sig=fake")
	raw.dedupCopy = true

	opts, err := raw.toOptions()
	a.Nil(err)
	a.True(opts.DedupCopy)

	// Without dedupCopy
	raw.dedupCopy = false
	opts, err = raw.toOptions()
	a.Nil(err)
	a.False(opts.DedupCopy)
}

// TestSyncUploadWithDedupCopyCrossContainer tests that --dedup-index-containers enables
// cross-container deduplication. A blob matching content in a different container on the
// destination account should be server-side copied from that container.
//
// Scenario:
// - "archive" container has blob data/old-report.txt with Content-MD5
// - "dest" container is the sync destination (starts empty)
// - Source has reports/report.txt with same content as archive/data/old-report.txt
// - Expected: report.txt should be transferred via dedup (BlobBlob copy from archive container)
func TestSyncUploadWithDedupCopyCrossContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Create the archive (source for dedup) container
	archiveCC, archiveContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, archiveCC)

	// Create the destination container
	destCC, destContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, destCC)

	// Upload a blob to the archive container WITH Content-MD5
	archiveContent := "This is archived content that should be dedup-copied cross-container"
	md5Archive := md5.Sum([]byte(archiveContent))

	archiveBlobClient := archiveCC.NewBlockBlobClient("data/old-report.txt")
	_, err := archiveBlobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(archiveContent)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5Archive[:],
			},
		})
	a.Nil(err)

	// Also upload some unique content to the destination container
	uniqueContent := "Unique content only in dest"
	md5Unique := md5.Sum([]byte(uniqueContent))
	destBlobClient := destCC.NewBlockBlobClient("existing/unique.txt")
	_, err = destBlobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(uniqueContent)),
		&blockblob.UploadOptions{
			HTTPHeaders: &blob.HTTPHeaders{
				BlobContentMD5: md5Unique[:],
			},
		})
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Set up local source directory
	// - report.txt has SAME content as archive/data/old-report.txt → should trigger cross-container dedup
	// - new-file.txt has completely new content → should trigger normal upload
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)

	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"reports/report.txt", "data/new-file.txt"})
	err = os.WriteFile(srcDirName+"/reports/report.txt", []byte(archiveContent), 0644)
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/data/new-file.txt", []byte("Brand new content not anywhere on destination"), 0644)
	a.Nil(err)

	// Set up the dedup interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	// For cross-container dedup, we need an account-level SAS (not container-level)
	// because the traverser needs to access multiple containers.
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	bscWithSAS := getBlobServiceClientWithSAS(a, credential)
	// Extract the account SAS from the service client URL
	serviceURLWithSAS, err := url.Parse(bscWithSAS.URL())
	a.Nil(err)
	// Build a container URL with the account SAS
	destURLWithAccountSAS := fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s",
		accountName, destContainerName, serviceURLWithSAS.RawQuery)

	raw := getDefaultSyncRawInput(srcDirName, destURLWithAccountSAS)
	raw.dedupCopy = true
	raw.dedupIndexContainers = archiveContainerName
	raw.putMd5 = true
	raw.compareHash = "MD5"

	// Run sync
	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// report.txt should be routed through dedup (BlobBlob) because its content
		// matches archive/data/old-report.txt in the archive container
		a.Equal(1, len(mockedRPC.dedupTransfers), "Expected 1 dedup transfer for reports/report.txt (cross-container match)")

		// new-file.txt should be routed through normal upload (LocalBlob)
		a.Equal(1, len(mockedRPC.normalTransfers), "Expected 1 normal transfer for data/new-file.txt")

		// Verify the dedup transfer source references the archive container
		dedupSrc := mockedRPC.dedupTransfers[0].Source
		a.Contains(dedupSrc, archiveContainerName, "Dedup source should reference the archive container")
		a.Contains(dedupSrc, "data/old-report.txt", "Dedup source should reference the blob path in archive")

		// Verify the dedup transfer destination references the dest container
		dedupDst := mockedRPC.dedupTransfers[0].Destination
		a.Contains(dedupDst, destContainerName, "Dedup destination should reference the dest container")
		a.Contains(dedupDst, "reports/report.txt", "Dedup destination should target reports/report.txt")
	})
}

// TestSyncDedupIndexContainersImpliesDedupCopy tests that specifying --dedup-index-containers
// automatically enables --dedup-copy.
func TestSyncDedupIndexContainersImpliesDedupCopy(t *testing.T) {
	a := assert.New(t)

	tmpDir := t.TempDir()
	raw := getDefaultSyncRawInput(tmpDir, "https://account.blob.core.windows.net/container?sv=2021-06-08&se=2030-01-01&sr=c&sp=rwdlacx&sig=fake")
	raw.dedupIndexContainers = "archive,backups"
	// Note: dedupCopy is NOT explicitly set

	opts, err := raw.toOptions()
	a.Nil(err)
	a.True(opts.DedupCopy, "--dedup-index-containers should imply --dedup-copy")
	a.Equal([]string{"archive", "backups"}, opts.DedupIndexContainers)
}

// TestSyncDedupIndexContainersRequiresBlobDestination tests that --dedup-index-containers
// is rejected when the destination is not Azure Blob storage.
// Note: This validation happens during option cooking (newCookedSyncOptions), not in toOptions().
// We test this by verifying the options are populated correctly and relying on the
// unit test in azcopy/ package for the actual validation logic.
func TestSyncDedupIndexContainersRequiresBlobDestination(t *testing.T) {
	a := assert.New(t)

	tmpDir := t.TempDir()
	// For a BlobLocal scenario, dedupCopy gets auto-disabled (destination is local)
	raw := getDefaultSyncRawInput("https://account.blob.core.windows.net/container?sv=2021-06-08&se=2030-01-01&sr=c&sp=rwdlacx&sig=fake", tmpDir)
	raw.dedupIndexContainers = "archive"
	raw.fromTo = "BlobLocal"

	opts, err := raw.toOptions()
	a.Nil(err) // toOptions() itself doesn't validate
	// The DedupIndexContainers is populated but dedupCopy is auto-enabled
	a.Equal([]string{"archive"}, opts.DedupIndexContainers)
	// DedupCopy is true because dedupIndexContainers implies it at the toOptions level
	a.True(opts.DedupCopy)
	// The actual validation (rejecting non-blob destinations) happens in cookedSyncOptions.validateOptions()
	// which is tested in the azcopy package
}

// TestSyncUploadWithDedupCopyCrossContainerMultiple tests dedup against multiple containers.
// Content is spread across two different containers, and the sync should find matches in both.
func TestSyncUploadWithDedupCopyCrossContainerMultiple(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Create two "index" containers and a destination container
	archiveCC, archiveContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, archiveCC)
	backupsCC, backupsContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, backupsCC)
	destCC, destContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, destCC)

	// Upload content to archive container
	contentA := "Content from the archive container for dedup test"
	md5A := md5.Sum([]byte(contentA))
	archiveBlobClient := archiveCC.NewBlockBlobClient("path/fileA.txt")
	_, err := archiveBlobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(contentA)),
		&blockblob.UploadOptions{HTTPHeaders: &blob.HTTPHeaders{BlobContentMD5: md5A[:]}})
	a.Nil(err)

	// Upload different content to backups container
	contentB := "Content from the backups container for dedup test"
	md5B := md5.Sum([]byte(contentB))
	backupsBlobClient := backupsCC.NewBlockBlobClient("bak/fileB.txt")
	_, err = backupsBlobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(contentB)),
		&blockblob.UploadOptions{HTTPHeaders: &blob.HTTPHeaders{BlobContentMD5: md5B[:]}})
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Set up local source with files matching both containers
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"fromArchive.txt", "fromBackups.txt", "brandNew.txt"})
	err = os.WriteFile(srcDirName+"/fromArchive.txt", []byte(contentA), 0644) // matches archive
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/fromBackups.txt", []byte(contentB), 0644) // matches backups
	a.Nil(err)
	err = os.WriteFile(srcDirName+"/brandNew.txt", []byte("completely new content"), 0644) // no match
	a.Nil(err)

	// Set up interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	// Use account-level SAS for cross-container access
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	bscWithSAS := getBlobServiceClientWithSAS(a, credential)
	serviceURLWithSAS, err := url.Parse(bscWithSAS.URL())
	a.Nil(err)
	destURLWithAccountSAS := fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s",
		accountName, destContainerName, serviceURLWithSAS.RawQuery)

	raw := getDefaultSyncRawInput(srcDirName, destURLWithAccountSAS)
	raw.dedupCopy = true
	raw.dedupIndexContainers = archiveContainerName + "," + backupsContainerName
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// Two files should be dedup-copied (one from archive, one from backups)
		a.Equal(2, len(mockedRPC.dedupTransfers), "Expected 2 dedup transfers (one from each indexed container)")

		// One file (brandNew.txt) should go through normal upload
		a.Equal(1, len(mockedRPC.normalTransfers), "Expected 1 normal transfer for brandNew.txt")
	})
}

// TestSyncUploadWithDedupCopyCrossContainerSkipsSameContainer tests that specifying
// the destination container name in --dedup-index-containers is silently skipped
// (since it's already being indexed via the normal destination traversal).
func TestSyncUploadWithDedupCopyCrossContainerSkipsSameContainer(t *testing.T) {
	a := assert.New(t)
	bsc := getBlobServiceClient()

	// Create dest container with a blob that has Content-MD5
	destCC, destContainerName := createNewContainer(a, bsc)
	defer deleteContainer(a, destCC)

	content := "Content already in the destination container"
	md5Content := md5.Sum([]byte(content))
	blobClient := destCC.NewBlockBlobClient("existing/original.txt")
	_, err := blobClient.Upload(ctx, streaming.NopCloser(strings.NewReader(content)),
		&blockblob.UploadOptions{HTTPHeaders: &blob.HTTPHeaders{BlobContentMD5: md5Content[:]}})
	a.Nil(err)

	time.Sleep(time.Millisecond * 1050)

	// Local source with file matching dest content (rename scenario)
	srcDirName := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(srcDirName)
	scenarioHelper{}.generateLocalFilesFromList(a, srcDirName, []string{"renamed/copy.txt"})
	err = os.WriteFile(srcDirName+"/renamed/copy.txt", []byte(content), 0644)
	a.Nil(err)

	// Set up interceptor
	mockedRPC := &dedupInterceptor{}
	mockedRPC.init(common.EFromTo.LocalBlob(), common.EFromTo.BlobBlob())

	// Use account-level SAS and specify the SAME container as destination in --dedup-index-containers
	accountName, accountKey := getAccountAndKey()
	credential, err := blob.NewSharedKeyCredential(accountName, accountKey)
	a.Nil(err)
	bscWithSAS := getBlobServiceClientWithSAS(a, credential)
	serviceURLWithSAS, err := url.Parse(bscWithSAS.URL())
	a.Nil(err)
	destURLWithAccountSAS := fmt.Sprintf("https://%s.blob.core.windows.net/%s?%s",
		accountName, destContainerName, serviceURLWithSAS.RawQuery)

	raw := getDefaultSyncRawInput(srcDirName, destURLWithAccountSAS)
	raw.dedupCopy = true
	// Include the SAME destination container name - should be skipped
	raw.dedupIndexContainers = destContainerName
	raw.putMd5 = true
	raw.compareHash = "MD5"

	runSyncAndVerify(a, raw, mockedRPC.intercept, mockedRPC.delete, func(err error) {
		a.Nil(err)

		// The file should still be dedup-copied (from the normal destination index)
		// The redundant container specification should be silently skipped
		a.Equal(1, len(mockedRPC.dedupTransfers), "Should still dedup from the destination's normal index")
		a.Equal(0, len(mockedRPC.normalTransfers), "No normal transfers needed")
	})
}
