# Infraestrutura como Código (IaC)

Este repositório contém os arquivos necessários para criar e gerenciar recursos na AWS usando CloudFormation.

### Rodar a Stack Setup (setup.yml)

Para criar o Bucket que armazena as funções Lambdas:

```bash
aws cloudformation deploy `
   --template-file "setup.yml" `
   --stack-name "SetupHealthMachineStack" `
   --capabilities "CAPABILITY_NAMED_IAM" `
```

### Subir os arquivos no Bucket

```bash
aws s3 cp ./lambda/setup s3://setup-bucket-199917718936/ --recursive
```

### Rodar a Stack Instance (instance.yml)

```bash
aws cloudformation deploy `
   --template-file "instance.yml" `
   --stack-name "InstanceHMStack" `
   --capabilities "CAPABILITY_NAMED_IAM" `
   --parameter-overrides "file://instance.json"
```

### Rodar a Stack Api (api.yml)

```bash
aws cloudformation deploy `
   --template-file "api.yml" `
   --stack-name "ApiHMStack" `
   --capabilities "CAPABILITY_NAMED_IAM" `
   --parameter-overrides "file://api.json"
```

### Rodar a Stack S3 e Lambda (s3-lambda.yml)

```bash
aws cloudformation deploy `
   --template-file "s3-lambda.yml" `
   --stack-name "S3LambdaHMStack" `
   --capabilities "CAPABILITY_NAMED_IAM" `
   --parameter-overrides "file://s3-lambda.json"
```

### Deletar a Stack

```bash
aws cloudformation delete-stack --stack-name HealthMachineStack
```

### Verificar a Stack

```bash
aws cloudformation describe-stack-events --stack-name HealthMachineStack
```

### Atualizar Lambda

```bash
aws lambda update-function-code `
  --function-name trigger_raw_to_trusted `
  --s3-bucket setup-bucket-199917718936 `
  --s3-key trigger_raw_to_trusted.zip

```

## Remover arquivos S3

```bash
aws s3 rm s3://raw-bucket-199917718936/ --recursive
aws s3 rm s3://trusted-bucket-199917718936/ --recursive
aws s3 rm s3://client-bucket-199917718936/ --recursive
```
