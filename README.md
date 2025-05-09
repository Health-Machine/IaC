# Infraestrutura como Código (IaC)

Este repositório contém os arquivos necessários para criar e gerenciar recursos na AWS usando CloudFormation.

### Rodar a Stack

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
