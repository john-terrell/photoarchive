function (doc) {
    if (doc.type === 'image') {
          emit(doc.SourceFile,1);
    }
}