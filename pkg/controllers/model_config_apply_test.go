package controllers

import (
	"testing"

	v1 "github.com/gorizond/koldun/pkg/apis/koldun.gorizond.io/v1"
	fakeapply "github.com/rancher/wrangler/v3/pkg/apply/fake"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestEnsureMetadataConfigMapIncludesObjectStorage(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &modelHandler{apply: applySpy}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
		Spec: v1.ModelSpec{
			SourceURL: "https://example.com/model",
			LocalPath: "/models/mistral",
			ObjectStorage: &v1.ModelObjectStorageSpec{
				Endpoint:         "https://minio.local",
				BucketForSource:  "source-bucket",
				BucketForConvert: "convert-bucket",
				SecretRef: &v1.SecretReference{
					Name:      "minio-creds",
					Namespace: "models",
				},
			},
		},
	}

	require.NoError(t, handler.ensureMetadataConfigMap(model))
	require.Len(t, applySpy.Objects, 1, "metadata ConfigMap should be applied once")
	objects := applySpy.Objects[0].All()
	require.Len(t, objects, 1)

	cm, ok := objects[0].(*corev1.ConfigMap)
	require.True(t, ok, "expected ConfigMap object")
	require.Equal(t, "mistral-metadata", cm.Name)
	require.Equal(t, "models", cm.Namespace)
	require.Equal(t, "https://example.com/model", cm.Data["sourceUrl"])
	require.Equal(t, "/models/mistral", cm.Data["localPath"])
	require.Equal(t, "https://minio.local", cm.Data["objectStorageEndpoint"])
	require.Equal(t, "source-bucket", cm.Data["objectStorageBucketForSource"])
	require.Equal(t, "convert-bucket", cm.Data["objectStorageBucketForConvert"])
	require.Equal(t, "minio-creds", cm.Data["objectStorageSecret"])
	require.Equal(t, "minio-creds", cm.Data["cacheSecret"], "legacy cacheSecret should be populated for compatibility")
}

func TestEnsureScriptConfigMapCreatesDownloadScript(t *testing.T) {
	applySpy := &fakeapply.FakeApply{}
	handler := &modelHandler{apply: applySpy}

	model := &v1.Model{
		ObjectMeta: metav1.ObjectMeta{Name: "mistral", Namespace: "models"},
	}

	require.NoError(t, handler.ensureScriptConfigMap(model))
	require.Len(t, applySpy.Objects, 1)
	objects := applySpy.Objects[0].All()
	require.Len(t, objects, 1)

	cm, ok := objects[0].(*corev1.ConfigMap)
	require.True(t, ok)
	require.Equal(t, "mistral-download-script", cm.Name)
	require.Contains(t, cm.Data["download.py"], "import")
}
