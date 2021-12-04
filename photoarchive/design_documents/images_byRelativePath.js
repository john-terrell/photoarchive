function (doc) {
    if (doc.File.MIMEType.startsWith('image')) {
          emit(doc.File.RelativePath,1);
    }
}
