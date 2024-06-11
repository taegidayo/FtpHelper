using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace FtpTest.FTP
{

    public enum SearchType
    {
        GET_ALL_ELEMENTS,      // 해당 경로내 모든 directory & File ( Folder가 존재하면 내부 폴더 및 파일도.. )
        GET_FILES_IN_CURRENT_DIRECTORY,         // 해당 경로내 모든 directory & File
    }

    public enum FtpResult
    {
        UnknownError = -1,
        OK = 0,
        Timeout,
        CommandFail,

        /// <summary>
        /// 로컬에 파일이 없는 경우 발생
        /// </summary>
        FileNotFound,
        /// <summary>
        /// Uri에 잘못된 형식의 데이터가 들어갔을 때 발생(공백문자 등)
        /// </summary>
        UriFormatError,
        IpAddressInvalidValue,
        PortInvalidValue,
        FilePathError,
        PathFormatError,
        PathNullError,

        WebException,
        UriInvalidValue,
        DomainNotExist,
        ReciveFail,
        ConnectFail,
        MessageLengthLimitExceeded,
        RequestCanceled,

        LoginFail = 530,
        FileNotExist = 550,
        StorageIsFull = 552,
        FileNameNotAllowed=553,
    }

    public class UriInfo : ICloneable
    {
        /// <summary>
        /// 해당 FTP서버의 IP주소<br/>
        /// 공백문자만 입력시 Local로 취급(Defalt가 공백문자)
        /// </summary>
        public string IpAdress { get; set; } = "";
        /// <summary>
        /// Port Number, Default:21
        /// </summary>
        public int Port { get; set; } = 21;
        /// <summary>
        /// Ip주소와 Port번호를 제외한 주소 ex) /Path/Log/a.txt
        /// </summary>
        public string Path { get; set; } = String.Empty;

        /// <summary>
        /// FTP서버에 접속하기 위한 사용자 이름
        /// </summary>        
        public string Username { get; set; } = String.Empty;

        /// <summary>
        /// FTP서버에 접속하기 위한 사용자의 비밀번호
        /// </summary>
        public string Password { get; set; } = String.Empty;

        /// <summary>
        /// FTP서버와의 연결 유지시간을 설정함
        /// </summary>
        /// <value>
        /// 단위: ms 기본값: 30000(30초)
        /// </value>
        public int Timeout { get; set; } = 30000;

        public object Clone()
        {
            UriInfo cloneValue = MemberwiseClone() as UriInfo;

            if (cloneValue == null)
            {
                cloneValue = new UriInfo();
            }

            return cloneValue;
        }
    }
    public class FtpHelper
    {
        /// <summary>
        /// Transfer에서 전송실패시 재시도 횟수
        /// </summary>
        public static int MaxRetryCount { get; set; } = 5;

        /// <summary>
        /// Transfer에서 전송시 한번에 전송할 BufferSize
        /// </summary>
        public static int BufferSize { get; set; } = 4096;

        private enum URITYPE
        {
            Local = 1,
            FTP = 2,
            UNKNOWN = 3,
        }

        private static string UriPath(UriInfo _uri) => $@"ftp://{_uri.IpAdress}:{_uri.Port}/{_uri.Path}";

        /// <summary>
        /// source가 로컬이고 dest가 로컬이 아닌 경우 : 파일 업로드 <br />
        /// source가 로컬이 아니고 dest가 로컬인 경우 : 파일 다운로드 <br />
        /// 둘다 로컬이 아닌 경우 : Server 간 파일 전송(source->dest)
        /// 
        /// </summary>
        /// <param name="_sourceUri">전달 할 Uri</param>
        /// <param name="_destinationUri">전달 받을 Uri</param>
        /// <returns></returns>
        public static FtpResult FileTransfer(UriInfo _sourceUri, UriInfo _destinationUri)
        {
            try
            {
                //Server To Server -> 
                URITYPE source_type = GetUriType(_sourceUri);
                URITYPE dest_type = GetUriType( _destinationUri);

                if(source_type==URITYPE.UNKNOWN||dest_type==URITYPE.UNKNOWN)
                {
                    return FtpResult.UriInvalidValue;
                }
                else if (source_type == URITYPE.FTP && dest_type== URITYPE.FTP)
                {
                    return ServerToServerFileTrans(_sourceUri, _destinationUri);
                }
                // Download
                else if (source_type== URITYPE.FTP)
                {
                    return FileDownload(_sourceUri, _destinationUri);
                }
                // Upload
                else if (dest_type == URITYPE.FTP)
                {
                    return FileUpload(_sourceUri, _destinationUri);
                }

                return FtpResult.UriInvalidValue;
            }
            catch (Exception ex)
            {
                return ExceptionToFtpResult(ex);
            }
        }

        private static FtpResult FileUpload(UriInfo _sourceUri, UriInfo _destinationUri)
        {
            //todo : 업로드 전 파일 크기 확인 및 크기 미 일치 시 리트라이 기능 추가 
            int retryCount = 0;
            while (retryCount <= MaxRetryCount)
            {
                try
                {
                    FileInfo fileInfo = null;
                    fileInfo = new FileInfo(_sourceUri.Path);

                    FtpWebRequest ftpRequest = GetFtpRequest(_destinationUri);
                    ftpRequest.Method = WebRequestMethods.Ftp.UploadFile;

                    byte[] buffer = new byte[BufferSize];
                    int bytesRead;
                    int byteWritten = 0;
                    long fileSize = 0;
                    MakeDirectoryList(_destinationUri);
                    using (FileStream fs = fileInfo.OpenRead())
                    using (Stream stream = ftpRequest.GetRequestStream())
                    {
                        fileSize = fs.Length;
                        while ((bytesRead = fs.Read(buffer, 0, buffer.Length)) != 0)
                        {
                            stream.Write(buffer, 0, bytesRead);
                            byteWritten += bytesRead;

                            Thread.Sleep(1);
                        }
                    }
                    
                    long remoteFileSize = GetRemoteFileSize(_destinationUri);

                    if (remoteFileSize != fileSize)
                    {
                        if (retryCount >= MaxRetryCount)
                        {
                            return FtpResult.CommandFail;
                        }
                        else
                        {
                            retryCount++;
                            continue;
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                catch (Exception ex)
                {
                    DeleteFile(_destinationUri);

                    var result = CheckException(ex, retryCount++);
                    if (result != FtpResult.OK)
                    {
                        return result;
                    }
                }

            }
            return FtpResult.OK;
        }

        private static FtpResult FileDownload(UriInfo _sourceUri, UriInfo _destinationUri)
        {
            int retryCount = 0;
            while (retryCount <= MaxRetryCount)
            {
                try
                {
                    FtpWebRequest ftpRequest = GetFtpRequest(_sourceUri);
                    ftpRequest.Method = WebRequestMethods.Ftp.DownloadFile;

                    CreateLocalDirectory(_destinationUri);
                    long remoteFileSize = GetRemoteFileSize(_sourceUri);
                    long fileSize = 0;
                    using (FtpWebResponse response = (FtpWebResponse)ftpRequest.GetResponse())
                    using (Stream responseStream = response.GetResponseStream())
                    using (FileStream fs = new FileStream(_destinationUri.Path, FileMode.Create))
                    {

                        byte[] buffer = new byte[BufferSize];
                        int bytesRead = 0;

                        while ((bytesRead = responseStream.Read(buffer, 0, buffer.Length)) != 0)
                        {
                            fs.Write(buffer, 0, bytesRead);
                            Thread.Sleep(1);
                        }
                        fileSize = fs.Length;
                    }

                    if (remoteFileSize != fileSize)
                    {
                        if (retryCount >= MaxRetryCount)
                        {
                            return FtpResult.CommandFail;
                        }
                        else
                        {
                            retryCount++;
                            continue;
                        }
                    }
                    else
                    {
                        break;
                    }
                }
                catch (Exception ex)
                {
                    DeleteFile(_destinationUri);
                    var result = CheckException(ex, retryCount++);
                    if (result != FtpResult.OK)
                    {
                        return result;
                    }
                }
            }
            return FtpResult.OK;
        }

        private static FtpResult ServerToServerFileTrans(UriInfo _sourceUri, UriInfo _destinationUri)
        {
            int retryCount = 0;
            FtpWebRequest sourceRequest = null;
            FtpWebRequest destRequest = null;
            while (retryCount <= MaxRetryCount)
            {
                try
                {

                    sourceRequest = GetFtpRequest(_sourceUri);
                    destRequest = GetFtpRequest(_destinationUri);

                    byte[] buffer = new byte[BufferSize];

                    sourceRequest.Method = WebRequestMethods.Ftp.DownloadFile;
                    destRequest.Method = WebRequestMethods.Ftp.UploadFile;

                    int bytesRead;
                    MakeDirectoryList(_destinationUri);
                    using (FtpWebResponse sourceResponse = (FtpWebResponse)sourceRequest.GetResponse())
                    using (Stream sourceStream = sourceResponse.GetResponseStream())
                    using (Stream destinationStream = destRequest.GetRequestStream())
                    {
                        while ((bytesRead = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            destinationStream.Write(buffer, 0, bytesRead);
                            Thread.Sleep(1);
                        }
                        sourceStream.Close();
                        sourceResponse.Close();
                        destinationStream.Close();

                        long sourceFileSize = GetRemoteFileSize(_sourceUri);
                        long destFileSize = GetRemoteFileSize(_destinationUri);


                        if (sourceFileSize != destFileSize)
                        {
                            if (retryCount >= MaxRetryCount)
                            {
                                return FtpResult.CommandFail;
                            }
                            else
                            {
                                retryCount++;
                                continue;
                            }
                        }
                        else
                        {
                            break;
                        }
                    }

                }
                catch (Exception ex)
                {
                    DeleteFile(_destinationUri);

                    var result = CheckException(ex, retryCount++);
                    if (result != FtpResult.OK)
                    {
                        return result;
                    }
                }
            }

            return FtpResult.OK;
        }

        private static long GetRemoteFileSize(UriInfo _uri)
        {
            try
            {
                FtpWebRequest request = GetFtpRequest(_uri);
                request.Method = WebRequestMethods.Ftp.GetFileSize;

                using (FtpWebResponse response = (FtpWebResponse)request.GetResponse())
                {
                    return response.ContentLength;
                }
            }
            catch (WebException ex)
            {
                return -1;
            }
        }

        /// <summary>
        /// 검색범위 내의 폴더 및 파일을 조회
        /// _SearchElementName을 입력할 경우 범위내에서 이름과 같은 요소만을 가져옴.
        /// </summary>
        /// <param name="_uri">검색할 Uri정보</param>
        /// <param name="_type">검색 조건-디렉토리의 범위 및 종류</param>
        /// <param name="_resultPath">찾은 요소의 Path정보</param>
        /// <param name="_SearchElementName">찾고자 하는 파일의 이름 - 미입력시 검색조건 내 모든 요소를 가져옴</param>
        /// <returns></returns>
        public static FtpResult SearchFiles(UriInfo _uri, SearchType _type, out List<String> _resultPath, string _SearchElementName = "")
        {
            _resultPath = new List<String>();
            try
            {
                FtpWebRequest ftpRequest = GetFtpRequest(_uri);
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectoryDetails;

               
                FtpWebResponse response = (FtpWebResponse)ftpRequest.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.Default);
               
                string line = reader.ReadLine();
                while (string.IsNullOrEmpty(line)==false)
                {
                    string[] tokens = line.Split(new char[] { ' ' }, 9, StringSplitOptions.RemoveEmptyEntries);
                    string changedTime = tokens[0];

                    string fileType = tokens[2];
                    string name = tokens[3];

                    if (fileType == "<DIR>") // 디렉토리인 경우
                    {
                        if (_type == SearchType.GET_ALL_ELEMENTS)
                        {
                            if (name != "." && name != "..")
                            {
                                var tempUri = (UriInfo)_uri.Clone();
                                tempUri.Path += $"/{name}";


                                if (string.IsNullOrEmpty(_SearchElementName) || _SearchElementName == name)
                                {
                                    _resultPath.Add($"{_uri.Path}/{name}");
                                }

                                SearchFiles(tempUri, _type, out List<String> Dir, _SearchElementName);
                                _resultPath.AddRange(Dir);
                            }
                        }
                    }
                    else // 파일인 경우
                    {
                            if (_SearchElementName == "" || _SearchElementName == name)
                            {
                                //파일명과 Path가 일치할경우 Path만 추가.(파일명과 Path명이 일치할 경우 해당 파일을 가르키는 Path임.
                                if (name.Equals(tokens.Last()) == true ) _resultPath.Add($"{_uri.Path}");
                                else _resultPath.Add($"{_uri.Path}/{name}");

                            }
                    }
                    line = reader.ReadLine();
                }

                reader.Close();
                response.Close();

            }
            catch (Exception ex)
            {
                return ExceptionToFtpResult(ex);
            }

            return FtpResult.OK;

        }

        public static FtpResult DeleteFile(UriInfo _uri)
        {
            int retryCount = 0;
            while (retryCount <= MaxRetryCount)
            {
                try
                {
                    FtpWebRequest ftpRequest = GetFtpRequest(_uri);

                    // FTP JOB 설정
                    ftpRequest.Method = WebRequestMethods.Ftp.DeleteFile;

                    using (FtpWebResponse res = (FtpWebResponse)ftpRequest.GetResponse())
                    {
                        res.Close();
                        res.Dispose();
                    }

                    return FtpResult.OK;
                }
                catch (Exception ex)
                {
                    var result= CheckException(ex,retryCount++);
                    if (result != FtpResult.OK) return result;
                }
            }
            return FtpResult.CommandFail;
        }

        private static FtpResult RemoveDirectory(UriInfo _uri)
        {
            int retryCount = 0;
            while (retryCount <= MaxRetryCount)
            {
                try
                {

                    FtpWebRequest ftpRequest = GetFtpRequest(_uri);

                    // FTP JOB 설정
                    ftpRequest.Method = WebRequestMethods.Ftp.RemoveDirectory;

                    using (FtpWebResponse res = (FtpWebResponse)ftpRequest.GetResponse())
                    {
                        res.Close();
                        res.Dispose();
                    }

                    return FtpResult.OK;
                }
                catch (Exception ex)
                {
                    var result=CheckException(ex, retryCount++) ;
                    if (result != FtpResult.OK)
                    {
                        return result;
                    }
                }
            }
            return FtpResult.CommandFail;
        }

        public static FtpResult DeleteOldElements(UriInfo _uri, int _daysAgo) => DeleteOldElements(_uri, _daysAgo, out var _);

        private static FtpResult DeleteOldElements(UriInfo _uri, int _daysAgo,out List<string> remainElements)
        {
            remainElements = new List<string>();
            try
            {

                DateTime deleteTime=DateTime.Now.AddDays(-Math.Abs(_daysAgo));

                

                FtpWebRequest ftpRequest = GetFtpRequest(_uri);
                ftpRequest.Method = WebRequestMethods.Ftp.ListDirectoryDetails;


                FtpWebResponse response = (FtpWebResponse)ftpRequest.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.Default);

                string line = reader.ReadLine();
                while (!string.IsNullOrEmpty(line))
                {
                    string[] tokens = line.Split(new char[] { ' ' }, 9, StringSplitOptions.RemoveEmptyEntries);
                    string changedTimeString = tokens[0];

                    DateTime changedTime = DateTime.ParseExact(changedTimeString, "MM-dd-yy", CultureInfo.InvariantCulture);

                    string fileType = tokens[2];
                    string name = tokens[3];


                    if (fileType == "<DIR>") // 디렉토리인 경우
                    {
                        if (name != "." && name != "..")
                        {
                            var tempUri = (UriInfo)_uri.Clone();
                            tempUri.Path += $"/{name}";
                            DeleteOldElements(tempUri, _daysAgo, out List<string> tempRemainList);
                            if(tempRemainList.Count==0)//현재 폴더가 빈 폴더가 된 경우 해당 폴더를 삭제
                            {
                                RemoveDirectory(tempUri);
                            }

                        }
                    }
                    else // 파일인 경우
                    {
                        if(changedTime<=deleteTime)
                        {
                            var tempUri = (UriInfo)_uri.Clone();
                            if (name.Equals(tokens.Last())==false)// 찾은 경로가 파일명과 일치하지 않을 경우에만 Path를 추가.
                            {
                                tempUri.Path += $"/{name}";
                            }
                            DeleteFile(tempUri);
                        }
                        else
                        {
                            remainElements.Append(_uri+name);
                        }
                    }
                    line = reader.ReadLine();
                }

                reader.Close();
                response.Close();

            }
            catch (Exception ex)
            {
                return ExceptionToFtpResult(ex);
            }

            return FtpResult.OK;

        }

        private static void MakeDirectoryList(UriInfo _uri)
        {
            UriInfo tempUri = (UriInfo)_uri.Clone();
            tempUri.Path = "";
            var DirList = _uri.Path.Split(new string[] { @"/" }, StringSplitOptions.RemoveEmptyEntries).ToList();
            DirList.RemoveAt(DirList.Count - 1);

            foreach (var item in DirList)
            {
                tempUri.Path += $"{item}/";

                MakeDirectory(tempUri);
            }
        }

        /// <summary>
        /// ListDirectory를 조회하는 것보다 아래처럼 처리하는게 좋을거 같음 
        /// 파일 개수가 많아질 경우 문제 발생 소지가 있을거 같아 ListDirectory 제거 
        /// </summary>
        /// <param name="_uri">test</param>
        private static void MakeDirectory(UriInfo _uri)
        {
            try
            {
                FtpWebRequest ftpWebRequest = GetFtpRequest(_uri);
                ftpWebRequest.Method = WebRequestMethods.Ftp.MakeDirectory;

                using (FtpWebResponse response = (FtpWebResponse)ftpWebRequest.GetResponse())
                {
                    switch (response.StatusCode)
                    {
                        case FtpStatusCode.ActionNotTakenFilenameNotAllowed:
                            throw new NotImplementedException("MakeDirectory Error Code : FtpStatusCode.ActionNotTakenFilenameNotAllowed Fail");

                        case FtpStatusCode.PathnameCreated: //정상생성완료.

                            break;
                    }

                    using (StreamReader streamReader = new StreamReader(response.GetResponseStream()))
                    {
                        streamReader.ReadToEnd();
                    }
                }
            }
            catch (System.Net.WebException ex)
            {

            }
            catch (System.Exception ex)
            {
                if (!ex.Message.Contains("550")) //이미 해당 폴더가 있다는 표시임. 그냥 넘어감.
                {
                    throw;
                }
            }
        }

        private static FtpResult ExceptionToFtpResult(Exception ex)
        {
            Type exceptionType = ex.GetType();

            if (exceptionType == typeof(FileNotFoundException))
            {
                return FtpResult.FileNotFound;
            }
            else if (exceptionType == typeof(UriFormatException))
            {
                UriFormatException exception = (UriFormatException)ex;

                if (ex.Message.Contains("포트") || ex.Message.Contains("port"))
                {
                    return FtpResult.PortInvalidValue;
                }
                return FtpResult.UriFormatError;
            }
            else if (exceptionType == typeof(ArgumentException))
            {
                if (ex.Message == "경로의 형식이 잘못되었습니다." || ex.Message.ToLower().Contains("Illegal characters in path.")) return FtpResult.PathFormatError;
                else if (ex.Message == "경로는 빈 문자열이거나 모두 공백일 수 없습니다." || ex.Message.ToLower().Contains("Path cannot be the empty string or all whitespace.")) return FtpResult.PathNullError;
                return FtpResult.FilePathError;
            }
            else if (exceptionType == typeof(WebException))
            {
                WebException exception = (WebException)ex;
                FtpWebResponse response = (FtpWebResponse)exception.Response;
                switch (exception.Status)
                {
                    case WebExceptionStatus.NameResolutionFailure: return FtpResult.UriInvalidValue;

                    case WebExceptionStatus.ProtocolError:
                        {
                            if (ex.Message.Contains("530") == true) return FtpResult.LoginFail;

                            else if (ex.Message.Contains("550") == true) return FtpResult.FileNotExist;

                            else if (ex.Message.Contains("552") == true) return FtpResult.StorageIsFull;

                            else if (ex.Message.Contains("553") == true) return FtpResult.FileNameNotAllowed;

                            else return FtpResult.WebException;
                        }

                    case WebExceptionStatus.Timeout: return FtpResult.Timeout;

                    case WebExceptionStatus.ReceiveFailure: return FtpResult.ReciveFail;
                    case WebExceptionStatus.ConnectFailure: return FtpResult.ConnectFail;

                    case WebExceptionStatus.MessageLengthLimitExceeded: return FtpResult.MessageLengthLimitExceeded;

                    default:
                        return FtpResult.WebException;
                }
            }
            else
            {
                throw (ex);
            }
        }

        private static FtpResult CheckException(Exception ex, int retryCount)
        {


            if (ex.GetType() == typeof(WebException))
            {
                if ((((WebException)ex).Status == WebExceptionStatus.ProtocolError))
                {
                    if (ex.Message.Contains("530"))
                    {
                        return FtpResult.LoginFail;
                    }
                }

                //서버가 요청을 취소했을 경우 재 시도 없이 바로 종료.
                if ((((WebException)ex).Status == WebExceptionStatus.RequestCanceled))
                {
                    return FtpResult.RequestCanceled;
                }
            }

            if (retryCount >= MaxRetryCount)
            {
                return ExceptionToFtpResult(ex);
            }

            return FtpResult.OK;
        }

        private static void CreateLocalDirectory(UriInfo _uri)
        {
            string directoryPath = Path.GetDirectoryName(_uri.Path);
            if (Directory.Exists(directoryPath) == true) return;

            Directory.CreateDirectory(directoryPath);
        }

        private static URITYPE GetUriType(UriInfo _uri)
        {
            //URITYPE type = URITYPE.UNKNOWN;

            if (string.IsNullOrEmpty(_uri.Path) == true)
            {
                return URITYPE.UNKNOWN; 
            }

            if (string.IsNullOrEmpty(_uri.IpAdress) == true)
            {
                return URITYPE.Local ;
            }

            if (IPAddress.TryParse(_uri.IpAdress, out var ipAddress) == false) return URITYPE.FTP;

            return URITYPE.FTP;
        }


        private static FtpWebRequest GetFtpRequest(UriInfo _uri)
        {

            string ftpPath = UriPath(_uri);


            string user = _uri.Username;
            string pwd = _uri.Password;
            FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(ftpPath);


            ftpRequest.Timeout = _uri.Timeout;
            ftpRequest.UsePassive = false;

            if (string.IsNullOrEmpty(_uri.Username) == false)
            {
                ftpRequest.Credentials = new NetworkCredential(user, pwd);  // ID,PW로 로그인
            }

            
            return ftpRequest;
        }
    }
}
