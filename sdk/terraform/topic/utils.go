package topic

//func getIAMToken(ctx context.Context, d ResourceDataProxy, config *Config) (string, error) {
//	if d != nil {
//		res, ok := d.GetOk("iam_token")
//		if ok {
//			return res.(string), nil
//		}
//	}
//	if config.ServiceAccountKeyFile != "" {
//		response, err := config.sdk.CreateIAMToken(ctx)
//		if err != nil {
//			return "", err
//		}
//		return response.IamToken, nil
//	}
//	token, err := config.sdk.CreateIAMToken(ctx)
//	if err != nil {
//		return "", fmt.Errorf("cannot determine IAM token: please set 'iam_token' attr in this resource or use service account key file at provider level")
//	}
//	return token.IamToken, nil
//}
