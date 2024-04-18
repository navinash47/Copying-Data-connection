pipeline {

  agent {
    node {
      label 'helix_gpt'
    }
  }

  environment {
    IMAGE_TAG="${BUILD_NUMBER}"
    DOCKER_REGISTRY="aus-harboreg-01.bmc.com"
    IMAGE_NAME="helix-gpt/${BRANCH_NAME}/data-connection"
    REGISTRY_USER='robot$gptsvcci'
    REGISTRY_TOKEN=credentials('harbor-registry-helixgpt-robot-token')
  }

  options {
    disableConcurrentBuilds()
    buildDiscarder(logRotator(numToKeepStr: '10', daysToKeepStr: '10'))
  }

  stages {

    stage('Build Version') {
        when { anyOf { branch 'main'; branch 'support-*' ; branch '*-ci' } }
        steps {
          sh 'echo "release: ${BRANCH_NAME} \nbuild: ${IMAGE_TAG} \ncommit: ${GIT_COMMIT}" >> ./src/version.txt'
        }
    }

    stage('Build Image') {
      when { anyOf { branch 'main'; branch 'support-*' ; branch '*-ci' } }
      steps {
        sh '''
          echo "Building Image"
          docker build \
               --target production \
               --label com.bmc.dwp-docker-version="$(docker version --format '{{.Server.Version}}')" \
               --label com.bmc.dwp-buildx-version="$(docker buildx version)" \
               --label com.bmc.dwp-build-date=$(date -I) \
               --label com.bmc.dwp-build-server="$(hostname)" \
               -t "${IMAGE_NAME}" .
        '''
      }
    }

    stage('Unit Tests') {
      steps {
        sh """
          echo "Executing Tests"
          docker build \
               --target export-stage \
               -o . \
               .
        """
      }
    }

    stage('Sonar') {
      when { anyOf { branch 'main'; branch 'support-*' } }
      steps {
        echo "Analyzing source"
        sh '''
          docker run \
            --rm \
            -e SONAR_HOST_URL="http://vl-aus-dsm-dv01.bmc.com:9000" \
            -e SONAR_SCANNER_OPTS="-Dsonar.branch.name=${BRANCH_NAME}" \
            -v ".:/usr/src" \
            sonarsource/sonar-scanner-cli:4.8.1
        '''
      }
    }

    stage('Pull Request Analysis') {
      when { anyOf { branch 'PR-*'  } }
      steps {
        echo "Analyzing source"
        sh '''
          docker run \
            --rm \
            -e SONAR_HOST_URL="http://vl-aus-dsm-dv01.bmc.com:9000" \
            -e SONAR_SCANNER_OPTS="-Dsonar.pullrequest.key=${CHANGE_ID} -Dsonar.pullrequest.branch=${CHANGE_BRANCH} -Dsonar.pullrequest.base=${CHANGE_TARGET} -Dsonar.scm.revision=${GIT_COMMIT}" \
            -v ".:/usr/src" \
            sonarsource/sonar-scanner-cli:4.8.1
        '''
      }
    }

    stage('Push image and cleanup'){
      when { anyOf { branch 'main'; branch 'support-*' ; branch '*-ci' } }
      steps {
        sh '''
         echo "Login using robot account"
         echo $REGISTRY_TOKEN | docker login $DOCKER_REGISTRY -u $REGISTRY_USER --password-stdin

         echo "Tagging docker image"
         docker tag ${IMAGE_NAME} ${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
         docker tag ${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} ${DOCKER_REGISTRY}/${IMAGE_NAME}:latest

         echo "Pushing docker image to harbour"
         docker push ${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
         docker push ${DOCKER_REGISTRY}/${IMAGE_NAME}:latest

         echo "Removing docker image from jenkins Node"
         docker rmi ${DOCKER_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} ${DOCKER_REGISTRY}/${IMAGE_NAME}:latest
        '''
      }
    }
  }

  post {
    cleanup {
      cleanWs()
    }
  }
}
