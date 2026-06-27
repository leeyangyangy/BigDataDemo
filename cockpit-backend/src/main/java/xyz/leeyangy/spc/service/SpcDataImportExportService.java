package xyz.leeyangy.spc.service;

import org.springframework.web.multipart.MultipartFile;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

public interface SpcDataImportExportService {

    Map<String, Object> importFromExcel(MultipartFile file, Long paramVersionId, Long userId);

    void exportToExcel(Long paramVersionId, LocalDateTime startTime, LocalDateTime endTime,
                       HttpServletResponse response) throws IOException;

    void downloadTemplate(HttpServletResponse response) throws IOException;
}