package index

import (
	"bytes"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/Masterminds/semver"
	"github.com/ghodss/yaml"
	"k8s.io/helm/pkg/proto/hapi/chart"
	"k8s.io/helm/pkg/repo"
	"k8s.io/helm/pkg/urlutil"
)

// Index of a helm chart repository.
type Index struct {
	*repo.IndexFile
}

// Reader returns io.Reader for index.
func (idx *Index) Reader() (io.Reader, error) {
	b, err := idx.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return bytes.NewReader(b), nil
}

// MarshalBinary encodes index to a binary form.
func (idx *Index) MarshalBinary() (data []byte, err error) {
	return yaml.Marshal(idx)
}

// UnmarshalBinary decodes index from a binary form.
func (idx *Index) UnmarshalBinary(data []byte) error {
	i := &repo.IndexFile{}
	if err := yaml.Unmarshal(data, i); err != nil {
		return err
	}
	i.SortEntries()

	*idx = Index{IndexFile: i}
	return nil
}

// AddOrReplace is the same as Add but replaces the version if it exists instead
// of adding it to the list of versions.
func (idx *Index) AddOrReplace(md *chart.Metadata, filename, s3BaseURL, publishBaseURI, digest string) error {
	// TODO: this looks like a workaround.
	// Think how we can rework this in the future.
	// Ref: https://github.com/kubernetes/helm/issues/3230

	var s3url, url string
	if publishBaseURI == "" {
		publishBaseURI = s3BaseURL
	}
	if publishBaseURI != "" {
		var err error
		_, file := filepath.Split(filename)
		url, err = urlutil.URLJoin(publishBaseURI, file)
		if err != nil {
			url = filepath.Join(publishBaseURI, file)
		}
	}
	if s3BaseURL != "" {
		var err error
		_, file := filepath.Split(filename)
		s3url, err = urlutil.URLJoin(s3BaseURL, file)
		if err != nil {
			s3url = filepath.Join(s3BaseURL, file)
		}
	}
	cr := &repo.ChartVersion{
		S3URLs:   []string{s3url},
		URLs:     []string{url},
		Metadata: md,
		Digest:   digest,
		Created:  time.Now(),
	}

	// If no chart with such name exists in the index, just create a new
	// list of versions.
	entry, ok := idx.Entries[md.Name]
	if !ok {
		idx.Entries[md.Name] = repo.ChartVersions{cr}
		return nil
	}

	chartSemVer, err := semver.NewVersion(md.Version)
	if err != nil {
		return err
	}

	// If such version exists, replace it.
	for i, v := range entry {
		itemSemVer, err := semver.NewVersion(v.Version)
		if err != nil {
			return err
		}

		if chartSemVer.Equal(itemSemVer) {
			idx.Entries[md.Name][i] = cr
			return nil
		}
	}

	// Otherwise just add to the list of versions
	idx.Entries[md.Name] = append(entry, cr)
	return nil
}

// Delete removes chart version from index and returns deleted item.
func (idx *Index) Delete(name, version string) (*repo.ChartVersion, error) {
	for chartName, chartVersions := range idx.Entries {
		if chartName != name {
			continue
		}

		for i, chartVersion := range chartVersions {
			if chartVersion.Version == version {
				idx.Entries[chartName] = append(
					idx.Entries[chartName][:i],
					idx.Entries[chartName][i+1:]...,
				)
				return chartVersion, nil
			}
		}
	}

	return nil, fmt.Errorf("chart %s version %s not found in index", name, version)
}

// New returns a new index.
func New() *Index {
	return &Index{
		repo.NewIndexFile(),
	}
}
