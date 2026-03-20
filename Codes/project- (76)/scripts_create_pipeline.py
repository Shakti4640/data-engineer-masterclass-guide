# file: scripts/create_pipeline.py
# Creates the complete CI/CD pipeline: CodeCommit → CodeBuild → Deploy → Approve → Prod

import boto3
import json

REGION = "us-east-2"
ACCOUNT_ID = "[REDACTED:BANK_ACCOUNT_NUMBER]"
REPO_NAME = "quickcart-data-platform"


def create_codecommit_repo(cc_client):
    """Create the source code repository"""
    try:
        response = cc_client.create_repository(
            repositoryName=REPO_NAME,
            repositoryDescription="QuickCart Data Platform — Glue, Redshift, Athena, Step Functions",
            tags={
                "Platform": "data-lake",
                "ManagedBy": "platform-team"
            }
        )
        clone_url = response["repositoryMetadata"]["cloneUrlHttp"]
        print(f"✅ CodeCommit repo: {REPO_NAME}")
        print(f"   Clone: {clone_url}")
        return clone_url
    except cc_client.exceptions.RepositoryNameExistsException:
        print(f"ℹ️  Repo exists: {REPO_NAME}")
        repo = cc_client.get_repository(repositoryName=REPO_NAME)
        return repo["repositoryMetadata"]["cloneUrlHttp"]


def create_codebuild_project(cb_client, environment):
    """Create CodeBuild project for build + test + deploy"""
    project_name = f"quickcart-data-platform-{environment}"

    build_project = {
        "name": project_name,
        "description": f"Build, test, deploy data platform to {environment}",
        "source": {
            "type": "CODEPIPELINE",
            "buildspec": "buildspec.yml"
        },
        "artifacts": {
            "type": "CODEPIPELINE"
        },
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/amazonlinux2-x86_64-standard:4.0",
            "computeType": "BUILD_GENERAL1_MEDIUM",
            "environmentVariables": [
                {
                    "name": "ENVIRONMENT",
                    "value": environment,
                    "type": "PLAINTEXT"
                },
                {
                    "name": "AWS_ACCOUNT_ID",
                    "value": ACCOUNT_ID,
                    "type": "PLAINTEXT"
                }
            ],
            "privilegedMode": False
        },
        "serviceRole": f"arn:aws:iam::{ACCOUNT_ID}:role/CodeBuildDataPlatformRole",
        "timeoutInMinutes": 30,
        "cache": {
            "type": "S3",
            "location": f"quickcart-artifacts-{environment}/codebuild-cache"
        },
        "logsConfig": {
            "cloudWatchLogs": {
                "status": "ENABLED",
                "groupName": f"/codebuild/quickcart-data-platform-{environment}",
                "streamName": "build-log"
            }
        },
        "tags": [
            {"key": "Environment", "value": environment},
            {"key": "Platform", "value": "data-lake"}
        ]
    }

    try:
        cb_client.create_project(**build_project)
        print(f"✅ CodeBuild project: {project_name}")
    except cb_client.exceptions.ResourceAlreadyExistsException:
        print(f"ℹ️  CodeBuild project exists: {project_name}")

    return project_name


def create_codepipeline(cp_client, environment):
    """
    Create the full CI/CD pipeline
    
    STAGES:
    1. Source: CodeCommit main branch
    2. Build & Test: CodeBuild (lint + unit tests + package)
    3. Deploy to Staging: CloudFormation + Glue scripts + Redshift
    4. Staging Smoke Test: CodeBuild (verify deployment)
    5. Manual Approval: Team lead approves production deploy
    6. Deploy to Production: Same as staging
    7. Production Smoke Test: CodeBuild (verify + auto-rollback)
    """
    pipeline_name = f"quickcart-data-platform-{environment}"

    pipeline_definition = {
        "name": pipeline_name,
        "roleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/CodePipelineDataPlatformRole",
        "artifactStore": {
            "type": "S3",
            "location": f"quickcart-artifacts-{environment}"
        },
        "stages": [
            # ═══ STAGE 1: SOURCE ═══
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "CodeCommit",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "CodeCommit",
                            "version": "1"
                        },
                        "configuration": {
                            "RepositoryName": REPO_NAME,
                            "BranchName": "main",
                            "PollForSourceChanges": "false"
                            # Use EventBridge trigger instead of polling
                        },
                        "outputArtifacts": [{"name": "SourceOutput"}],
                        "runOrder": 1
                    }
                ]
            },

            # ═══ STAGE 2: BUILD & TEST ═══
            {
                "name": "BuildAndTest",
                "actions": [
                    {
                        "name": "LintAndUnitTest",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1"
                        },
                        "configuration": {
                            "ProjectName": f"quickcart-data-platform-{environment}",
                            "EnvironmentVariables": json.dumps([
                                {"name": "STAGE", "value": "build_and_test", "type": "PLAINTEXT"}
                            ])
                        },
                        "inputArtifacts": [{"name": "SourceOutput"}],
                        "outputArtifacts": [{"name": "BuildOutput"}],
                        "runOrder": 1
                    }
                ]
            },

            # ═══ STAGE 3: DEPLOY TO STAGING ═══
            {
                "name": "DeployStaging",
                "actions": [
                    {
                        "name": "CloudFormation-Staging",
                        "actionTypeId": {
                            "category": "Deploy",
                            "owner": "AWS",
                            "provider": "CloudFormation",
                            "version": "1"
                        },
                        "configuration": {
                            "ActionMode": "CREATE_UPDATE",
                            "StackName": "quickcart-glue-jobs-staging",
                            "TemplatePath": "BuildOutput::dist/infrastructure/glue-jobs.yaml",
                            "TemplateConfiguration": "BuildOutput::dist/infrastructure/params.json",
                            "Capabilities": "CAPABILITY_IAM,CAPABILITY_NAMED_IAM",
                            "RoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/CloudFormationDeployRole"
                        },
                        "inputArtifacts": [{"name": "BuildOutput"}],
                        "runOrder": 1
                    }
                ]
            },

            # ═══ STAGE 4: STAGING SMOKE TEST ═══
            {
                "name": "StagingSmokeTest",
                "actions": [
                    {
                        "name": "VerifyStaging",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1"
                        },
                        "configuration": {
                            "ProjectName": f"quickcart-data-platform-staging",
                            "EnvironmentVariables": json.dumps([
                                {"name": "STAGE", "value": "smoke_test", "type": "PLAINTEXT"},
                                {"name": "ENVIRONMENT", "value": "staging", "type": "PLAINTEXT"}
                            ])
                        },
                        "inputArtifacts": [{"name": "BuildOutput"}],
                        "runOrder": 1
                    }
                ]
            },

            # ═══ STAGE 5: MANUAL APPROVAL ═══
            {
                "name": "ProductionApproval",
                "actions": [
                    {
                        "name": "ApproveProduction",
                        "actionTypeId": {
                            "category": "Approval",
                            "owner": "AWS",
                            "provider": "Manual",
                            "version": "1"
                        },
                        "configuration": {
                            "NotificationArn": f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:pipeline-approvals",
                            "CustomData": "Staging smoke tests passed. Review CloudFormation changeset before approving production deploy."
                        },
                        "runOrder": 1
                    }
                ]
            },

            # ═══ STAGE 6: DEPLOY TO PRODUCTION ═══
            {
                "name": "DeployProduction",
                "actions": [
                    {
                        "name": "CloudFormation-Prod",
                        "actionTypeId": {
                            "category": "Deploy",
                            "owner": "AWS",
                            "provider": "CloudFormation",
                            "version": "1"
                        },
                        "configuration": {
                            "ActionMode": "CREATE_UPDATE",
                            "StackName": "quickcart-glue-jobs-prod",
                            "TemplatePath": "BuildOutput::dist/infrastructure/glue-jobs.yaml",
                            "TemplateConfiguration": "BuildOutput::dist/infrastructure/params.json",
                            "Capabilities": "CAPABILITY_IAM,CAPABILITY_NAMED_IAM",
                            "RoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/CloudFormationDeployRole"
                        },
                        "inputArtifacts": [{"name": "BuildOutput"}],
                        "runOrder": 1
                    }
                ]
            },

            # ═══ STAGE 7: PRODUCTION SMOKE TEST ═══
            {
                "name": "ProductionSmokeTest",
                "actions": [
                    {
                        "name": "VerifyProduction",
                        "actionTypeId": {
                            "category": "Build",
                            "owner": "AWS",
                            "provider": "CodeBuild",
                            "version": "1"
                        },
                        "configuration": {
                            "ProjectName": f"quickcart-data-platform-prod",
                            "EnvironmentVariables": json.dumps([
                                {"name": "STAGE", "value": "smoke_test", "type": "PLAINTEXT"},
                                {"name": "ENVIRONMENT", "value": "prod", "type": "PLAINTEXT"}
                            ])
                        },
                        "inputArtifacts": [{"name": "BuildOutput"}],
                        "runOrder": 1
                    }
                ]
            }
        ]
    }

    try:
        cp_client.create_pipeline(pipeline=pipeline_definition)
        print(f"✅ CodePipeline: {pipeline_name}")
        print(f"   Stages: Source → Build → Staging → Smoke → Approve → Prod → Verify")
    except cp_client.exceptions.PipelineNameInUseException:
        print(f"ℹ️  Pipeline exists: {pipeline_name}")
        # Update existing pipeline
        cp_client.update_pipeline(pipeline=pipeline_definition)
        print(f"   ✅ Pipeline updated")

    return pipeline_name


def create_eventbridge_trigger(events_client, pipeline_name):
    """
    Trigger pipeline on CodeCommit push to main branch
    → Replaces polling (faster, cheaper)
    """
    rule_name = f"codecommit-{REPO_NAME}-main"

    events_client.put_rule(
        Name=rule_name,
        EventPattern=json.dumps({
            "source": ["aws.codecommit"],
            "detail-type": ["CodeCommit Repository State Change"],
            "resources": [f"arn:aws:codecommit:{REGION}:{ACCOUNT_ID}:{REPO_NAME}"],
            "detail": {
                "event": ["referenceCreated", "referenceUpdated"],
                "referenceType": ["branch"],
                "referenceName": ["main"]
            }
        }),
        State="ENABLED",
        Description=f"Trigger pipeline on push to {REPO_NAME}/main"
    )

    events_client.put_targets(
        Rule=rule_name,
        Targets=[{
            "Id": "codepipeline-trigger",
            "Arn": f"arn:aws:codepipeline:{REGION}:{ACCOUNT_ID}:{pipeline_name}",
            "RoleArn": f"arn:aws:iam::{ACCOUNT_ID}:role/EventBridgeCodePipelineRole"
        }]
    )

    print(f"✅ EventBridge trigger: push to main → pipeline start")


def main():
    print("=" * 70)
    print("🔧 CI/CD PIPELINE SETUP")
    print("   CodeCommit → CodeBuild → CloudFormation → Production")
    print("=" * 70)

    cc_client = boto3.client("codecommit", region_name=REGION)
    cb_client = boto3.client("codebuild", region_name=REGION)
    cp_client = boto3.client("codepipeline", region_name=REGION)
    events_client = boto3.client("events", region_name=REGION)

    # Step 1: Repository
    print("\n--- Step 1: CodeCommit Repository ---")
    create_codecommit_repo(cc_client)

    # Step 2: CodeBuild projects
    print("\n--- Step 2: CodeBuild Projects ---")
    for env in ["dev", "staging", "prod"]:
        create_codebuild_project(cb_client, env)

    # Step 3: Pipeline
    print("\n--- Step 3: CodePipeline ---")
    pipeline_name = create_codepipeline(cp_client, "prod")

    # Step 4: EventBridge trigger
    print("\n--- Step 4: EventBridge Trigger ---")
    create_eventbridge_trigger(events_client, pipeline_name)

    print("\n" + "=" * 70)
    print("✅ CI/CD PIPELINE COMPLETE")
    print(f"""
   DEVELOPER WORKFLOW:
   1. git clone <repo-url>
   2. git checkout -b feature/my-change
   3. (make changes to Glue scripts / CloudFormation / Redshift SQL)
   4. make test-local  (runs unit tests locally)
   5. git push origin feature/my-change
   6. Create Pull Request → CI runs automatically
   7. Peer reviews + approves PR
   8. Merge to main → CD pipeline deploys to staging
   9. Team lead approves → deploys to production
   10. Smoke test verifies → done!

   ROLLBACK:
   python scripts/rollback.py --environment prod
   (or: python scripts/rollback.py --environment prod --target-sha abc12345)
    """)
    print("=" * 70)


if __name__ == "__main__":
    main()
