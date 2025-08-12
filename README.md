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

### Rodar a Stack Principal (cloudformation.yml)

Para criar ou atualizar a stack, execute o seguinte comando no terminal:

```bash
aws cloudformation deploy `
   --template-file "cloudformation.yml" `
   --stack-name "HealthMachineStack" `
   --capabilities "CAPABILITY_NAMED_IAM" `
   --parameter-overrides "file://params.json"
```

### Deletar a Stack

```bash
aws cloudformation delete-stack --stack-name HealthMachineStack
```

### Verificar a Stack

```bash
aws cloudformation describe-stack-events --stack-name HealthMachineStack
```
