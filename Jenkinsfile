pipeline {

    agent any

    stages {
        stage('cloudformation deploy') {
            agent { 
                docker {  image 'amazon/aws-sam-cli-build-image-python3.8:latest' }
            }
            steps{
                dir('./cloudformation-deployment'){
                    withAWS(credentials: 'xqf052aws1-cred', region:'ap-southeast-2'){
                        sh './deploy.sh'
                    }
                }
            }
        }


        stage('ansible') {
            steps{
                dir('./ansible'){                   
                    sh './ansible.sh'
                }
            }
        }

        stage('cloudformation destroy') {
            agent { 
                docker {  image 'amazon/aws-sam-cli-build-image-python3.8:latest' }
            }
            steps{
                dir('./cloudformation-deployment'){
                    withAWS(credentials: 'xqf052aws1-cred', region:'ap-southeast-2'){
                        sh './destroy.sh'
                    }
                }
            }
        }
    }

    
}