from aws_cdk import core as cdk

from stacks.pipeline_stack import PipelineStack
from stacks.application_stack import ApplicationStack


app = cdk.App()

region = 'eu-west-1'

account="429068853603"

env = cdk.Environment(account=account, region=region)

ApplicationStack(
    app, "emr-demo-cfn",
    description="emr-demo resources",
    env=env
)

PipelineStack(
    app, 'emr-demo-cfn-pipeline',
    description='emr-demo deployment pipeline',
    env=env
)

app.synth()
