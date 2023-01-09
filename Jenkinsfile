pipeline{
    agent any
    
    tools{
        jdk 'jdk8'
        maven 'Maven 3.8.7'
    }

    stages{
        stage('git'){
            steps{
               git branch: 'main', url: 'https://github.com/GKSSN/azure-storage.git'
            }
        }
        stage('build'){
            steps{
                sh "mvn clean install"
            }
        }
    }
}
