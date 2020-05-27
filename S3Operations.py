import boto3
import uuid 

s3_resource = boto3.resource('s3')

#create a bucket 
def create_bucket_name(bucket_prefix):
    return ''.join([bucket_prefix, str(uuid.uuid4())])

def create_bucket(bucket_prefix, s3_connection):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    bucket_response = s3_connection.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': current_region})
    return bucket_name, bucket_response

bucket_name, response = create_bucket(bucket_prefix='pythonbucketboto', s3_connection=s3_resource)

second_bucket_name = 'mys3bucketss'
file_name = 'file_name.txt'

#function to create a temporary file
def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name


s3_resource.meta.client.upload_file(Filename=file_name, Bucket=bucket_name, Key=file_name)

#uploading a file on s3 
s3_resource.Object(bucket_name, file_name).upload_file(Filename=file_name)

#downloading a file from s3
s3_resource.Object(bucket_name, file_name).download_file(f'/tmp/{file_name}')

#copying a file from one s3 bucket to another
def copy_to_bucket(bucket_from_name, bucket_to_name, file_name):
    copy_source = {
        'Bucket': bucket_from_name,
        'Key': file_name
    }
    s3_resource.Object(bucket_to_name, file_name).copy(copy_source)

copy_to_bucket(bucket_name, second_bucket_name, file_name)

#deleting the file from bucket
s3_resource.Object(second_bucket_name, file_name).delete()

#mentioning acl property with your object
second_file_name = create_temp_file(400, 'secondfile.txt', 's')
second_object = s3_resource.Object(bucket_name, second_file_name)
second_object.upload_file(second_file_name, ExtraArgs={'ACL': 'public-read'})

#changing acl again to default i.e. private
second_object_acl = second_object.Acl()
response = second_object_acl.put(ACL='private')
print(second_object_acl.grants)

#uploading a file with enabling encryption and specifying storage class
third_file_name = create_temp_file(300, 'thirdfile.txt', 't')
third_object = s3_resource.Object(bucket_name, third_file_name)
third_object.upload_file(third_file_name, ExtraArgs={
                         'ServerSideEncryption': 'AES256', 
                         'StorageClass': 'STANDARD_IA'})

#enable versioning of the bucket
def enable_bucket_versioning(bucket_name):
    bkt_versioning = s3_resource.BucketVersioning(bucket_name)
    bkt_versioning.enable()
    print(bkt_versioning.status)

enable_bucket_versioning(bucket_name)

#uploading files using versioning
s3_resource.Object(bucket_name, file_name).upload_file(file_name)
s3_resource.Object(bucket_name, file_name).upload_file(third_file_name)

print(s3_resource.Object(bucket_name, file_name).version_id)

#Traversing all the buckets in your account and getting their name
for bucket in s3_resource.buckets.all():
    print(bucket.name)

#getting reference of the bucket object and bucket itself
first_bucket = s3_resource.Bucket(name=bucket_name)
first_object = s3_resource.Object(bucket_name=bucket_name, key=file_name)

#If you have a Bucket variable, you can create an Object directly:
first_object_again = first_bucket.Object(file_name)

# if you have an Object variable, then you can get the Bucket
first_bucket_again = first_object.Bucket()

#list all the objects from a bucket
for obj in first_bucket.objects.all():
    print(obj.key)

#to access the object sub resource
for obj in first_bucket.objects.all():
    subsrc = obj.Object()
    print(obj.key, obj.storage_class, obj.last_modified,subsrc.version_id, subsrc.metadata)

#delete all objects from a bucket
def delete_all_objects(bucket_name):
    res = []
    bucket=s3_resource.Bucket(bucket_name)
    for obj_version in bucket.object_versions.all():
        res.append({'Key': obj_version.object_key,
                    'VersionId': obj_version.id})
    print(res)
    bucket.delete_objects(Delete={'Objects': res})

#delete the bucket
s3_resource.Bucket(bucket_name).delete()
