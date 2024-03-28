import graphene


class Query(graphene.ObjectType):
    sign_path = graphene.String(private_path=graphene.String(required=True))

    def resolve_sign_path(root, info, private_path):
        return info.context["s3"].generate_presigned_url(private_path)


schema = graphene.Schema(query=Query)
