#!/bin/bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO] $1${NC}"; }
log_warn() { echo -e "${YELLOW}[WARN] $1${NC}"; }
log_error() { echo -e "${RED}[ERROR] $1${NC}"; }
log_header() { echo -e "${BLUE}=== $1 ===${NC}"; }
log_detail() { echo -e "${PURPLE}[DETAIL] $1${NC}"; }

# --- Variabili Mutevoli ---
NAME="salesforce-update-terminated"
LAMBDA_NAME_PART="SalesforceUpdateTerminat"

# --- Variabili ---
AWS_ACCOUNT_ID_DEV="786142130037"
AWS_ACCOUNT_ID_PROD="086971354489"
AWS_REGION="eu-west-1"
LOCAL_IMAGE_NAME="${NAME}:latest"
ECR_REPOSITORY_NAME="lambda/${NAME}"
PROFILE_NAME_DEV="dev-deploy"
PROFILE_NAME_PROD="prod-deploy"

# URI completi dei repository
REMOTE_TAG_DEV="${AWS_ACCOUNT_ID_DEV}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY_NAME}:latest"
REMOTE_TAG_PROD="${AWS_ACCOUNT_ID_PROD}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY_NAME}:latest"

# --- Selezione ambiente ---
log_header "Selezione Ambiente di Deploy"
echo -e "${YELLOW}Seleziona l'ambiente di deploy:${NC}"
echo "1) DEV"
echo "2) PROD"
echo "3) Entrambi (DEV + PROD)"
echo ""
read -p "Scelta [1-3]: " ENV_CHOICE

case $ENV_CHOICE in
    1)
        DEPLOY_DEV=true
        DEPLOY_PROD=false
        log_info "✅ Deploy selezionato: DEV"
        ;;
    2)
        DEPLOY_DEV=false
        DEPLOY_PROD=true
        log_info "✅ Deploy selezionato: PROD"
        ;;
    3)
        DEPLOY_DEV=true
        DEPLOY_PROD=true
        log_info "✅ Deploy selezionato: DEV + PROD"
        ;;
    *)
        log_error "❌ Scelta non valida. Uscita."
        exit 1
        ;;
esac

echo ""

# --- Funzione per deploy su un ambiente ---
deploy_to_environment() {
    local ENV_NAME=$1
    local AWS_ACCOUNT_ID=$2
    local PROFILE_NAME=$3
    local REMOTE_TAG=$4

    log_header "Deploy ambiente ${ENV_NAME}"

    log_info "📦 Verifying ECR repository exists..."
    if ! aws ecr describe-repositories --profile $PROFILE_NAME --repository-names $ECR_REPOSITORY_NAME --region $AWS_REGION > /dev/null 2>&1; then
        log_info "⚠️ ECR repository '$ECR_REPOSITORY_NAME' does not exist, creating..."
        if aws ecr create-repository --profile $PROFILE_NAME \
               --repository-name $ECR_REPOSITORY_NAME --region $AWS_REGION \
               --image-scanning-configuration scanOnPush=true \
               --image-tag-mutability MUTABLE > /dev/null 2>&1; then
            log_info "✅ ECR repository created successfully"
        else
            log_error "❌ Failed to create ECR repository"
            log_info "💡 Check ECR permissions for creating repositories"
            exit 1
        fi
    else
        log_info "✅ ECR repository exists: $ECR_REPOSITORY_NAME"
        aws --profile $PROFILE_NAME ecr describe-repositories \
            --repository-names $ECR_REPOSITORY_NAME \
            --region $AWS_REGION \
            --query 'repositories[0].{URI:repositoryUri,Created:createdAt}' \
            --output table
    fi

    # Login a ECR
    log_info "🔐 Login to ECR..."
    aws ecr get-login-password \
        --profile $PROFILE_NAME \
        --region ${AWS_REGION} | \
        docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

    # Tagga l'immagine
    log_info "🏷️ Tagging repository ${ENV_NAME}: ${REMOTE_TAG}"
    docker tag ${LOCAL_IMAGE_NAME} ${REMOTE_TAG}

    # Push
    log_info "⬆️ Push ECR per ${ENV_NAME}"
    docker push ${REMOTE_TAG}

    log_header "Deploy Lambda Function"

    # Cerca la Lambda function che contiene LAMBDA_NAME_PART nel nome
    log_info "🔍 Searching for Lambda function containing '${LAMBDA_NAME_PART}'..."

    MATCHING_FUNCTIONS=$(aws lambda list-functions \
        --profile $PROFILE_NAME \
        --region $AWS_REGION \
        --query "Functions[?contains(FunctionName, '${LAMBDA_NAME_PART}')].FunctionName" \
        --output text)

    # Conta quante funzioni corrispondono
    FUNCTION_COUNT=$(echo "$MATCHING_FUNCTIONS" | wc -w | tr -d ' ')

    if [ "$FUNCTION_COUNT" -eq 0 ]; then
        log_error "❌ No Lambda function found containing '${LAMBDA_NAME_PART}' in ${ENV_NAME}"
        log_info "💡 Make sure the Lambda function is deployed via Amplify first"
    elif [ "$FUNCTION_COUNT" -gt 1 ]; then
        log_error "❌ Multiple Lambda functions found containing '${LAMBDA_NAME_PART}':"
        echo "$MATCHING_FUNCTIONS" | tr '\t' '\n' | while read func; do
            echo "   - $func"
        done
        log_info "💡 Please use a more specific LAMBDA_NAME_PART to match exactly one function"
        exit 1
    fi

    LAMBDA_FUNCTION_NAME=$(echo "$MATCHING_FUNCTIONS" | tr -d '\t\n ')
    log_info "✅ Found unique Lambda function: $LAMBDA_FUNCTION_NAME"

    # Aggiorna il codice della Lambda function
    log_info "🔄 Updating Lambda function code..."
    if aws lambda update-function-code \
        --profile $PROFILE_NAME \
        --function-name $LAMBDA_FUNCTION_NAME \
        --image-uri $REMOTE_TAG \
        --region $AWS_REGION > /dev/null 2>&1; then
        log_info "✅ Lambda function code updated successfully"
    else
        log_error "❌ Failed to update Lambda function code"
        exit 1
    fi

    log_info "⏳ Waiting for function update to complete..."
    aws lambda wait function-updated \
        --profile $PROFILE_NAME \
        --function-name $LAMBDA_FUNCTION_NAME \
        --region $AWS_REGION

    log_info "✅ Deploy ${ENV_NAME} completato con successo!"
    log_info "📋 Lambda function deployed: $LAMBDA_FUNCTION_NAME"

    echo ""
}

# --- Build dell'immagine ---
log_header "Build Docker Image"
log_info "🔨 Building l'immagine: ${LOCAL_IMAGE_NAME}..."
docker buildx build --platform linux/amd64 --provenance=false -t ${LOCAL_IMAGE_NAME} .
log_info "✅ Build completata"
echo ""

# --- Deploy negli ambienti selezionati ---
if [ "$DEPLOY_DEV" = true ]; then
    deploy_to_environment "DEV" "$AWS_ACCOUNT_ID_DEV" "$PROFILE_NAME_DEV" "$REMOTE_TAG_DEV"
fi

if [ "$DEPLOY_PROD" = true ]; then
    deploy_to_environment "PROD" "$AWS_ACCOUNT_ID_PROD" "$PROFILE_NAME_PROD" "$REMOTE_TAG_PROD"
fi

# --- Pulizia immagini locali Docker ---
log_header "Cleanup immagini Docker locali"

log_info "📦 Rimuovendo immagine locale ${LOCAL_IMAGE_NAME}"
docker rmi ${LOCAL_IMAGE_NAME} || log_warn "Impossibile rimuovere ${LOCAL_IMAGE_NAME} (potrebbe essere ancora in uso)"

if [ "$DEPLOY_DEV" = true ]; then
    log_info "📦 Rimuovendo tag DEV locale ${REMOTE_TAG_DEV}"
    docker rmi ${REMOTE_TAG_DEV} || log_warn "Impossibile rimuovere tag ${REMOTE_TAG_DEV}"
fi

if [ "$DEPLOY_PROD" = true ]; then
    log_info "📦 Rimuovendo tag PROD locale ${REMOTE_TAG_PROD}"
    docker rmi ${REMOTE_TAG_PROD} || log_warn "Impossibile rimuovere tag ${REMOTE_TAG_PROD}"
fi

log_info "🧹 Rimuovendo immagini dangling/non utilizzate"
docker image prune -f || log_warn "Impossibile fare prune delle immagini"

log_info "✅ Pulizia completata"
log_header "Deploy completato! 🎉"