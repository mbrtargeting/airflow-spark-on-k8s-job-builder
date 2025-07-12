from enum import Enum


class Arch(Enum):
    amd64 = {"key": "kubernetes.io/arch", "operator": "In", "values": ["amd64"]}
    arm64 = {"key": "kubernetes.io/arch", "operator": "In", "values": ["arm64"]}


class K8sZone(Enum):
    eu_central_1a = {
        "key": "topology.kubernetes.io/zone",
        "operator": "In",
        "values": ["eu-central-1a"],
    }
    eu_central_1b = {
        "key": "topology.kubernetes.io/zone",
        "operator": "In",
        "values": ["eu-central-1b"],
    }
    eu_central_1c = {
        "key": "topology.kubernetes.io/zone",
        "operator": "In",
        "values": ["eu-central-1c"],
    }


class CapacityType(Enum):
    on_demand = {
        "key": "karpenter.sh/capacity-type",
        "operator": "In",
        "values": ["on-demand"],
    }
    spot = {
        "key": "karpenter.sh/capacity-type",
        "operator": "In",
        "values": ["spot"],
    }


OVERRIDE_ME = "TODO_OVERRIDE_ME"
SPARK_S3A = "spark.hadoop.fs.s3a"
SPARK_K8S_AUTH = "spark.kubernetes.authenticate.submission"
WEB_IDENT_PROVIDER = "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
DEFAULT_SPARK_CONF = {
    f"{SPARK_S3A}.aws.credentials.provider": WEB_IDENT_PROVIDER,
    f"{SPARK_S3A}.path.style.access": "true",
    f"{SPARK_K8S_AUTH}.caCertFile": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
    f"{SPARK_K8S_AUTH}.oauthTokenFile": "/var/run/secrets/kubernetes.io/serviceaccount/token",
    "spark.kubernetes.driver.service.deleteOnTermination": "true",
}
DEFAULT_SPARK_VERSION = "3.4.2"
DEFAULT_NAMESPACE = "default"

SPARK_JOB_SPEC_TEMPLATE = {
    "params": {
        "on_finish_action": "delete_pod",
        "jobName": OVERRIDE_ME,
        "namespace": "default",
        "language": "Scala",
        "dockerImage": "gcr.io/spark/spark",
        "dockerImageTag": "v3.4.2",
        # example: "mainClass": "com.example.dataplatform.MyApp
        "mainClass": OVERRIDE_ME,
        # example: "mainApplicationFile": "local:///app/my-app.jar"
        "mainApplicationFile": OVERRIDE_ME,
        "sparkVersion": DEFAULT_SPARK_VERSION,
        "sparkConf": DEFAULT_SPARK_CONF,
        "jobArguments": [],
        "imagePullSecrets": [],
        # https://kubeflow.github.io/spark-operator/docs/user-guide.html#specifying-application-dependencies
        "deps": {},
        "volumes": [],
        "driver": {
            "serviceAccount": OVERRIDE_ME,
            "cores": 1,
            "memory": "2g",
            "tolerations": [],
            "volumeMounts": [],
            "sidecars": [],
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    CapacityType.on_demand.value,
                                ]
                            }
                        ],
                    },
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "preference": {
                                "matchExpressions": [
                                    Arch.arm64.value,
                                ]
                            },
                        }
                    ],
                },
            },
            "annotations": {
                "karpenter.sh/do-not-evict": "true",
                "karpenter.sh/do-not-consolidate": "true",
                "karpenter.sh/do-not-disrupt": "true",
            },
            "labels": {"version": "3.4.2"},
            "secrets": [],
            "env": [],
        },
        "executor": {
            "instances": 2,
            "serviceAccount": OVERRIDE_ME,
            "cores": 2,
            "memory": "4g",
            "tolerations": [],
            "volumeMounts": [],
            "sidecars": [],
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [{"matchExpressions": [CapacityType.spot.value]}],
                    },
                    "preferredDuringSchedulingIgnoredDuringExecution": [
                        {
                            "weight": 100,
                            "preference": {
                                "matchExpressions": [
                                    Arch.arm64.value,
                                ]
                            },
                        }
                    ],
                },
            },
            "annotations": {
                "karpenter.sh/do-not-evict": "true",
                "karpenter.sh/do-not-consolidate": "true",
                "karpenter.sh/do-not-disrupt": "true",
            },
            "labels": {},
            "secrets": [],
        },
    },
}
