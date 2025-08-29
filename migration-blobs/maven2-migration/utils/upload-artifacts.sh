#!/bin/bash

# 🔧 Настройки
NEXUS_URL="https://nexus.sanich.tech:8443"
REPO_ID="maven2-test"

GROUP_ID="com.example"
ARTIFACT_ID="demo-lib"
ARTIFACT_DIR="./artifacts"

# Проверяем директорию
if [ ! -d "$ARTIFACT_DIR" ]; then
    echo "❌ Директория $ARTIFACT_DIR не найдена!"
    exit 1
fi

# Функция загрузки
upload_artifact() {
    local version="$1"
    local jar_file="$2"
    local pom_file="$3"

    echo "🚀 Загружаем: $version → $REPO_ID"
    echo "📦 JAR: $jar_file"
    echo "📋 POM: $pom_file"

    mvn deploy:deploy-file \
        -DgroupId="$GROUP_ID" \
        -DartifactId="$ARTIFACT_ID" \
        -Dversion="$version" \
        -Dpackaging=jar \
        -Dfile="$jar_file" \
        -DpomFile="$pom_file" \
        -DrepositoryId="$REPO_ID" \
        -Durl="${NEXUS_URL}/repository/${REPO_ID}" \
        -DgeneratePom=false \
        -DuniqueVersion=false
}

# Основной цикл
echo "📂 Загружаем артефакты из: $ARTIFACT_DIR"
echo "=========================================="

for pom_file in "$ARTIFACT_DIR"/*.pom; do
    if [ -f "$pom_file" ]; then
        filename=$(basename "$pom_file")
        version="${filename#$ARTIFACT_ID-}"
        version="${version%.pom}"

        jar_file="$ARTIFACT_DIR/$ARTIFACT_ID-$version.jar"

        if [ -f "$jar_file" ]; then
            upload_artifact "$version" "$jar_file" "$pom_file"

            if [ $? -eq 0 ]; then
                echo "✅ Успешно: $version"
            else
                echo "❌ Ошибка: $version"
            fi
        else
            echo "❌ Нет JAR для $version"
        fi
    fi
done

echo "=========================================="
echo "🏁 Все загрузки завершены!"
