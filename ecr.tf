resource "aws_ecr_repository" "dataworks-snapshot-sender-status-checker" {
  name = "dataworks-snapshot-sender-status-checker"
  tags = merge(
    local.common_tags,
    { DockerHub : "dwpdigital/dataworks-snapshot-sender-status-checker" }
  )
}

resource "aws_ecr_repository_policy" "dataworks-snapshot-sender-status-checker" {
  repository = aws_ecr_repository.dataworks-snapshot-sender-status-checker.name
  policy     = data.terraform_remote_state.management.outputs.ecr_iam_policy_document
}

output "ecr_example_url" {
  value = aws_ecr_repository.dataworks-snapshot-sender-status-checker.repository_url
}
