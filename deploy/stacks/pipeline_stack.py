from aws_cdk import (
    core as cdk,
    aws_iam as _iam,
    aws_s3 as _s3,
    aws_codepipeline as _codepipeline,
    aws_codepipeline_actions as _codepipeline_actions,
    aws_codebuild as _codebuild,
    aws_kms as _kms,
    aws_secretsmanager as _sm
)

from aws_cdk.aws_codepipeline import IStage
from aws_cdk.core import Tags


class PipelineStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.pipeline_role = self._create_pipeline_role()
        self.pipeline = self._create_pipeline()
        self.event_role = self._create_event_role()
        self.source_artifact = _codepipeline.Artifact("Source")
        self._add_github_source_stage()
        self._add_test_stage()
        self._add_deploy_stage()

    def _create_pipeline_role(self):
        role = _iam.Role(
            self, "CodePipelineRole",
            role_name="sid-emr-demo-role-pipeline",
            description="role assumed by Codebuild and codepipeline to deploye",
            assumed_by=_iam.CompositePrincipal(
                _iam.ServicePrincipal("codebuild.amazonaws.com"),
                _iam.ServicePrincipal("codepipeline.amazonaws.com")
            )
        )
        role.add_managed_policy(
            _iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        )
        return role

    def _create_pipeline(self):
        return _codepipeline.Pipeline(
            self, "Pipeline",
            pipeline_name="emr-demo-pipeline",
            artifact_bucket=self._create_artifacts_bucket(),
            role=self.pipeline_role
        )

    def _create_event_role(self):
        if self.pipeline is None:
            raise ValueError('first create pipeline')

        role = _iam.Role(
            self, "CodeCommitEventRole",
            role_name="emr-demo-role-event",
            description="event role assumed by AWS Events to trigger pipeline",
            assumed_by=_iam.ServicePrincipal("events.amazonaws.com")
        )
        role.add_to_policy(
            _iam.PolicyStatement(
                effect=_iam.Effect.ALLOW,
                resources=[self.pipeline.pipeline_arn],
                actions=["codepipeline:StartPipelineExecution"]
            )
        )
        return role

    def _create_artifacts_bucket(self) -> _s3.Bucket:
        artifacts_bucket = _s3.Bucket(
            self, "DeploymentArtifactsBucket",
            bucket_name="sid-emr-deployment-artifacts",
            encryption=_s3.BucketEncryption.S3_MANAGED,
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            enforce_ssl=True,
            versioned=True
        )
        artifacts_bucket.node.default_child.object_lock_enabled = True
        return artifacts_bucket

    def _generate_codebuild_action(
            self,
            action_name: str,
            buildspec_filename: str = None,
            buildspec_object: dict = None,
            env_vars_str: dict = None,
            env_vars_secrets: dict = None,
            run_order: int = None
    ) -> _codepipeline_actions.CodeBuildAction:
        if (buildspec_filename and buildspec_object) or (not buildspec_filename and not buildspec_object):
            raise ValueError('Provide singel buildspec')
        env_vars_str = env_vars_str or {}
        env_vars_secrets = env_vars_secrets or {}

        if buildspec_filename:
            build_spec = _codebuild.BuildSpec.from_source_filename(buildspec_filename)
        else:
            build_spec = _codebuild.BuildSpec.from_object(buildspec_object)

        project = _codebuild.PipelineProject(
            self, f"Codebuild{action_name}Project",
            project_name=f"sid-emr-demo-codebuild-{action_name}",
            environment=_codebuild.BuildEnvironment(build_image=_codebuild.LinuxBuildImage.STANDARD_5_0),
            environment_variables={
                **{
                    key: _codebuild.BuildEnvironmentVariable(value=val)
                        for key, val in env_vars_str.items()
                },
                **{
                    key: _codebuild.BuildEnvironmentVariable(
                        value=val,
                        type=_codebuild.BuildEnvironmentVariableType.SECRETS_MANAGER
                    ) for key, val in env_vars_secrets.items()
                }
            },
            build_spec=build_spec,
            role=self.pipeline_role
        )
        return _codepipeline_actions.CodeBuildAction(
            action_name=action_name,
            project=project,
            input=self.source_artifact,
            run_order=run_order,
            role=self.pipeline_role
        )

    def _generate_codebuild_python_action(
            self,
            action_name: str,
            python_path: str = '.',
            python_dependencies: str = None,
            commands: list = None,
            env_vars_str: dict = None,
            env_vars_secrets: dict = None,
            run_order: int = None
    ) -> _codepipeline_actions.CodeBuildAction:
        build_spec_object = {
            'versions': '0.2',
            'phases': {
                'install': {
                    'commands': [f"pip install {python_dependencies}"] if python_dependencies else []
                },
                'pre_build': {
                    'commands': [
                        'python --version',
                        'aws sts get-caller-identity'
                    ]
                },
                'build': {
                    'commands': [f"export PYTHONPATH={python_path}"] + commands
                }
            }
        }
        return self._generate_codebuild_action(
            action_name=action_name,
            buildspec_object=build_spec_object,
            env_vars_str=env_vars_str,
            env_vars_secrets=env_vars_secrets,
            run_order=run_order
        )

    def _add_deploy_stage(self) -> IStage:
        return self.pipeline.add_stage(
            stage_name="Deploy",
            actions=[
                self._generate_codebuild_python_action(
                    action_name="PauseDAGs",
                    python_path="deploy",
                    python_dependencies='dynaconf',
                    commands=["python deploy/cicd/pause_dags.py"],
                    run_order=1
                ),
                self._generate_codebuild_action(
                    action_name="CdkDeploy",
                    buildspec_filename='deploy/cicd/cdk_deploy_buildspec.yaml',
                    env_vars_str={
                        'STACK_NAMES': 'sid-emr-test-cfn'
                    },
                    run_order=2
                ),
                self._generate_codebuild_action(
                    action_name="ETLDeploy",
                    buildspec_filename='deploy/cicd/etl_deploy_buildspec.yaml',
                    run_order=3
                ),
                self._generate_codebuild_python_action(
                    action_name="UnpauseDAGs",
                    python_path='deploy',
                    commands=['python deploy/cicd/unpase_dags.py'],
                    run_order=4
                )
            ]
        )

    def _add_test_stage(self) -> IStage:
        return self.pipeline.add_stage(
            stage_name="Test",
            actions=[
                self._generate_codebuild_action(
                    action_name="ETLTest",
                    buildspec_filename='deploy/cicd/etl_test_buildspec.yaml'
                )
            ]
        )

    def assume_deployment_role(self):
        return ['aws sts get-caller-identity']

    def _add_github_source_stage(self) -> IStage:
        # secrets_key = _kms.Key.from_key_arn(self, "SourceSecretsKey", "arn:aws:kms:eu-west-1:429068853603:key/af1b077a-428f-4780-810c-abda7fb7709b")
        # secrets_key.grant_decrypt(self.pipeline_role)
        # github_ssh_key = _sm.Secret.from_secret_arn(self, "GitHubSSHKey", 'arn:aws:secretsmanager:eu-west-1:429068853603:secret:emr-demo-source-code-ssh-key-ucbtl1')
        # github_ssh_key.grant_read(self.pipeline_role)
        source_action = _codepipeline_actions.CodeStarConnectionsSourceAction(
            action_name="Source",
            role=self.pipeline_role,
            owner='sidkalra',
            repo='emr-demo',
            output=self.source_artifact,
            connection_arn="arn:aws:codestar-connections:us-east-1:429068853603:connection/48edf3ac-9ab5-4562-9ec5-1852e8cd45cf",
            branch="master"
        )
        return self.pipeline.add_stage(
            stage_name="Source",
            actions=[source_action]
        )

    def _create_artifacts_bucket(self) -> _s3.Bucket:
        bucket = _s3.Bucket(
            self, "DeployementsArtifactsBucket",
            bucket_name="emr-demo-s3-deployment-artifacts",
            block_public_access=_s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        return bucket

