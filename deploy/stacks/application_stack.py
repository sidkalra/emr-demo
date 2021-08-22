from aws_cdk import (
    core as cdk,
    aws_iam as _iam,
    aws_s3 as _s3,
    aws_kms as _kms
)


class ApplicationStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.application_role = self._create_application_role()
        self.application_instance_profile = self._create_application_instance_profile()
        self.data_encryption_key = self._create_data_encryption_key()
        # self.data_bucket = self._create_encrypted_data_bucket()
        # self._grant_read_write(self.application_role)


    def _create_data_encryption_key(self):
        return _kms.Key(
            self, "DataEncryptionKey",
            alias="sid-emr-demo-key",
            description="KMS key for dataset data encryption",
            enable_key_rotation=False
        )

    def _create_encrypted_data_bucket(self):
        data_bucket = _s3.Bucket(
            self, "DataBucket",
            bucket_name="sid-emr-test-bucket",
            encryption=_s3.BucketEncryption.KMS,
            encryption_key=self.data_encryption_key,
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            object_ownership=_s3.ObjectOwnership.BUCKET_OWNER_PREFERRED,
            enforce_ssl=True,
            versioned=True
        )
        return data_bucket

    def _create_application_role(self) -> _iam.Role:
        application_role = _iam.Role(
            self, "ApplicationRole",
            role_name="sid-emr-application-role",
            assumed_by=_iam.ServicePrincipal("ec2.amazonaws.com")
        )
        return application_role

    def _grant_read_write(self, principal: _iam.IPrincipal):
        self.data_bucket.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                resources=[self.data_bucket.bucket_arn],
                principals=[principal],
                actions=["s3:List*"]
            )
        )
        self.data_bucket.add_to_resource_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                resources=[self.data_bucket.bucket_arn+'/*'],
                principals=[principal],
                actions=["s3:Get*", "s3:Put*", "s3:DeleteObject*", "s3:Abort*"]
            )
        )
        self.data_bucket.grant_read_write(self.application_role)
        self.data_bucket.grant_put_acl(self.application_role)

    def _create_application_instance_profile(self) -> _iam.CfnInstanceProfile:
        if self.application_role is None:
            raise ValueError('Application role needs to be created first')
        return _iam.CfnInstanceProfile(
            self, "ApplicationInstanceProfile",
            roles=[self.application_role.role_name],
            instance_profile_name=self.application_role.role_name
        )
