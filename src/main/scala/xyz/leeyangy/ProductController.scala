package xyz.leeyangy

import org.springframework.http.{HttpStatus, ResponseEntity}
import org.springframework.web.bind.annotation._
import scala.collection.mutable.ListBuffer

/**
 * @ProjectName : SparkLearn
 * @Package : xyz.leeyangy
 * @ClassName : ProductController
 * @Author : leeyangy
 * @CreateTime : 2026/4/11 00:15
 * @Version : 1.0
 * @Description : 
 * @Modify_log : 
 */

@RestController
@RequestMapping(Array("/products"))
class ProductController {
  private val products = new ListBuffer[Product]
  @PostMapping
  def addProduct(@RequestBody product: Product): ResponseEntity[String] = {
    products += product
    ResponseEntity.status(HttpStatus.CREATED).body("Product added successfully.")
  }
  @DeleteMapping(Array("/{id}"))
  def deleteProduct(@PathVariable id: Int): ResponseEntity[String] = {
    val removed = products.remove(id)
    if (removed != null)
      ResponseEntity.ok("Product deleted successfully.")
    else
      ResponseEntity.status(HttpStatus.NOT_FOUND).body(s"Product with id $id not found.")
  }
  @GetMapping
  def getAllProducts: Seq[Product] = {
    products.toSeq
  }
}
case class Product(id: Int, name: String, price: Double)
