#include "primer/trie.h"
#include <memory>
#include <string_view>
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  if (root_ == nullptr) {
    return nullptr;
  }
  auto ptr = root_;
  for (auto ch : key) {
    auto children = ptr->children_;
    if (children.find(ch) == children.end()) {
      return nullptr;
    }
    ptr = children[ch];
  }

  auto vnode_ptr = dynamic_cast<const TrieNodeWithValue<T> *>(ptr.get());
  if (vnode_ptr == nullptr) {
    return nullptr;
  }
  return vnode_ptr->value_.get();

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  auto value_ptr = std::make_shared<T>(std::move(value));
  auto ptr = std::make_shared<TrieNode>();
  if (root_ != nullptr) {
    ptr = std::shared_ptr<TrieNode>(root_->Clone());
  }
  if (key.empty()) {
    return Trie(std::make_shared<TrieNodeWithValue<T>>(ptr->children_, value_ptr));
  }

  auto ntrie = Trie(ptr);  // new Trie to be returned

  for (size_t i = 0; i < key.size() - 1; i++) {
    auto ch = key[i];
    auto &children = ptr->children_;
    if (children.find(ch) != children.end()) {
      ptr = std::shared_ptr<TrieNode>(children[ch]->Clone());
    } else {
      ptr = std::make_shared<TrieNode>();
    }
    children[ch] = ptr;
  }

  auto ch = key.back();
  auto &children = ptr->children_;
  if (children.find(ch) != children.end()) {
    children[ch] = std::make_shared<const TrieNodeWithValue<T>>(children[ch]->children_, value_ptr);
  } else {
    children[ch] = std::make_shared<const TrieNodeWithValue<T>>(value_ptr);
  }

  return ntrie;

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  auto ptr = std::shared_ptr<TrieNode>();
  if (root_ != nullptr) {
    ptr = std::shared_ptr<TrieNode>(root_->Clone());
  }
  if (key.empty()) {
    return Trie(std::make_shared<TrieNode>(ptr->children_));
  }

  auto ntrie = Trie(ptr);  // new Trie may be returned

  auto haskey = RemoveFrom(ptr, key);
  if (!haskey) {
    return *this;
  }
  return ntrie;

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

auto Trie::RemoveFrom(std::shared_ptr<TrieNode> ptr, std::string_view key) const -> bool {
  BUSTUB_ASSERT(!key.empty(), "RemoveFrom should only deal with non-empty key");
  auto ch = key.front();
  auto &children = ptr->children_;
  // the key does not exist
  if (children.find(ch) == children.end()) {
    return false;
  }
  // recursion
  if (key.size() > 1) {
    ptr = std::shared_ptr<TrieNode>(children[ch]->Clone());
    children[ch] = ptr;
    key.remove_prefix(1);
    RemoveFrom(ptr, key);
  } else {
    children[ch] = std::make_shared<TrieNode>(children[ch]->children_);
  }
  // remove node that has no value and children
  if (children[ch]->children_.empty() && !children[ch]->is_value_node_) {
    children.erase(ch);
  }

  return true;
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
