#!/bin/bash

# === Конфигурация ===
GROUP_ID="com.example"
ARTIFACT_ID="demo-lib"
VERSIONS=("1.0" "2.0")
OUTPUT_DIR="./artifacts"

# === Java-код ===
JAVA_CODE='public class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello from demo-lib!");
    }
}'

mkdir -p ${OUTPUT_DIR}

for VERSION in "${VERSIONS[@]}"; do
  BUILD_DIR="./build-${VERSION}"
  mkdir -p "${BUILD_DIR}/src/main/java/com/example"

  # 1. Создаём Java файл
  echo "$JAVA_CODE" > "${BUILD_DIR}/src/main/java/com/example/HelloWorld.java"

  # 2. Создаём POM
  cat > "${BUILD_DIR}/pom.xml" <<EOF
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
                             http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>${GROUP_ID}</groupId>
  <artifactId>${ARTIFACT_ID}</artifactId>
  <version>${VERSION}</version>
</project>
EOF

  # 3. Собираем JAR
  (
    cd "$BUILD_DIR"
    mvn clean package
  )

  # 4. Копируем JAR и POM в artifacts/
  cp "${BUILD_DIR}/target/${ARTIFACT_ID}-${VERSION}.jar" "${OUTPUT_DIR}/"
  cp "${BUILD_DIR}/pom.xml" "${OUTPUT_DIR}/${ARTIFACT_ID}-${VERSION}.pom"

  # 5. Удаляем build
  rm -rf "$BUILD_DIR"
done

echo "✅ Артефакты созданы в папке: $OUTPUT_DIR"
